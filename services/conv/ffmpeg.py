import os
from inspect import cleandoc

import nebula
from nebula.ffmpeg import FFmpeg
from nebula.storages import storages

from .common import BaseEncoder, ConversionError, temp_file
from .overlays import Overlay


class FilterChain:
    filters: list[str | Overlay]

    def __init__(self, *filters: str | Overlay) -> None:
        self.filters = list(filters)

    def render(self) -> str:
        return ",".join([f"{f}" for f in self.filters])

    def __str__(self) -> str:
        return self.render()


class NebulaFFMPEG(BaseEncoder):
    def configure(self) -> None:
        self.proc = None
        self.ffmpeg: FFmpeg | None = None
        self.files = {}
        self.ffparams = ["-y"]
        self.ffparams.extend(["-i", self.asset.file_path])
        asset = self.asset
        params = self.params

        # Shared namespace for task scripts and expressions, so variables
        # defined in <script> are visible to subsequent evals.
        # (exec() no longer writes to function locals since Python 3.13)
        ns = {**globals(), "self": self, "asset": asset, "params": params}

        def add_param(p) -> None:
            # Top-level and paramset params alike: indented text is fine,
            # ss goes before -i (input seeking)
            ptext = cleandoc(p.text.strip()) if p.text else ""
            value = str(eval(ptext, ns)) if p.text else ""
            if p.attrib["name"] == "ss":
                self.ffparams.insert(1, "-ss")
                self.ffparams.insert(2, value)
            else:
                self.ffparams.append("-" + p.attrib["name"])
                if value:
                    self.ffparams.append(value)

        for p in self.task:
            ns["p"] = p
            if p.tag == "param":
                add_param(p)

            elif p.tag == "metadata":
                ptext = cleandoc(p.text.strip()) if p.text else ""
                if not ptext:
                    continue
                try:
                    tags = eval(ptext, ns)
                except Exception as e:
                    nebula.log.traceback()
                    raise ConversionError("Error in task 'metadata' element.") from e

                if not isinstance(tags, dict):
                    raise ConversionError("Metadata must evaluate to a dictionary")

                # 'stream' attribute may be used to target a specific stream
                # or a chapter, e.g. <metadata stream="s:a:0">

                specifier = p.attrib.get("stream")
                arg = f"-metadata:{specifier}" if specifier else "-metadata"

                for key, value in tags.items():
                    if value is None or value == "":
                        continue
                    self.ffparams.extend([arg, f"{key}={value}"])

            elif p.tag == "script":
                if p.text:
                    try:
                        exec(p.text, ns)
                    except Exception as e:
                        nebula.log.traceback()
                        raise ConversionError("Error in task 'pre' script.") from e

            elif p.tag == "paramset" and eval(p.attrib["condition"], ns):
                for pp in p.findall("param"):
                    add_param(pp)

            elif p.tag == "output":
                id_storage = int(eval(p.attrib["storage"], ns))
                storage = storages[id_storage]
                if not storage.is_writable:
                    raise ConversionError("Target storage is not writable")

                if not p.text:
                    raise ConversionError("Output path is not specified")

                target_rel_path = eval(p.text, ns)

                if not target_rel_path:
                    raise ConversionError("Output path is empty")

                target_path = os.path.join(
                    storages[id_storage].local_path, target_rel_path
                )
                target_dir = os.path.split(target_path)[0]

                temp_ext = os.path.splitext(target_path)[1].lstrip(".")
                temp_path = temp_file(id_storage, temp_ext)

                if not temp_path:
                    raise ConversionError("Unable to create temp directory")

                if not os.path.isdir(target_dir):
                    try:
                        os.makedirs(target_dir)
                    except Exception as e:
                        nebula.log.traceback()
                        raise ConversionError(
                            f"Unable to create output directory {target_dir}"
                        ) from e

                if not p.attrib.get("direct", False):
                    self.files[temp_path] = target_path
                    self.ffparams.append(temp_path)
                else:
                    self.ffparams.append(target_path)

    @property
    def is_running(self) -> bool:
        return self.ffmpeg is not None and self.ffmpeg.is_running

    def start(self) -> None:
        self.ffmpeg = FFmpeg(*self.ffparams)
        nebula.log.info(f"Executing ffmpeg {' '.join(self.ffmpeg.args)}")
        self.ffmpeg.start()
        self.proc = self.ffmpeg.proc

    def stop(self) -> None:
        if self.is_running:
            assert self.ffmpeg
            self.aborted = True
            self.ffmpeg.stop()

    def wait(self, progress_handler) -> None:
        assert self.ffmpeg
        duration = self.asset["duration"]

        def handler(position: float) -> None:
            progress_handler((position / duration) * 100 if duration else 0)

        self.ffmpeg.wait(handler)

    def finalize(self) -> None:
        if not self.ffmpeg:
            return

        # != 0: a process killed by a signal has a negative code
        if self.ffmpeg.returncode != 0:
            last_lines = self.ffmpeg.error_lines[-10:]
            nebula.log.debug("FFMPEG error log:")
            for line in last_lines:
                nebula.log.debug(line)

            lmsg = last_lines[-1] if last_lines else "Unknown error"
            lmsg = lmsg.strip()
            if lmsg:
                lmsg = f" ({lmsg})"
            raise ConversionError(f"Encoding failed{lmsg}")

        for temp_path, target_path in self.files.items():
            try:
                nebula.log.debug(f"Moving {temp_path} to {target_path}")
                os.rename(temp_path, target_path)
            except OSError as e:
                raise ConversionError("Unable to move output file") from e
