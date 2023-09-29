import os

from nxtools import FFMPEG

import nebula
from nebula.storages import storages

from .common import BaseEncoder, ConversionError, temp_file


class NebulaFFMPEG(BaseEncoder):
    def configure(self) -> None:
        self.files = {}
        self.ffparams = ["-y"]
        self.ffparams.extend(["-i", self.asset.file_path])
        asset = self.asset
        params = self.params
        assert asset
        assert params is not None

        for p in self.task:
            if p.tag == "param":
                value = str(eval(p.text)) if p.text else ""
                if p.attrib["name"] == "ss":
                    self.ffparams.insert(1, "-ss")
                    self.ffparams.insert(2, value)
                else:
                    self.ffparams.append("-" + p.attrib["name"])
                    if value:
                        self.ffparams.append(value)

            elif p.tag == "script":
                if p.text:
                    try:
                        exec(p.text)
                    except Exception:
                        nebula.log.traceback()
                        raise ConversionError("Error in task 'pre' script.")

            elif p.tag == "paramset" and eval(p.attrib["condition"]):
                for pp in p.findall("param"):
                    value = str(eval(pp.text)) if pp.text else ""
                    self.ffparams.append("-" + pp.attrib["name"])
                    if value:
                        self.ffparams.append(value)

            elif p.tag == "output":
                id_storage = int(eval(p.attrib["storage"]))
                storage = storages[id_storage]
                if not storage.is_writable:
                    raise ConversionError("Target storage is not writable")

                target_rel_path = eval(p.text)
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
                    except Exception:
                        nebula.log.traceback()
                        raise ConversionError(
                            f"Unable to create output directory {target_dir}"
                        )

                if not p.attrib.get("direct", False):
                    self.files[temp_path] = target_path
                    self.ffparams.append(temp_path)
                else:
                    self.ffparams.append(target_path)

    @property
    def is_running(self) -> bool:
        return self.proc and self.proc.is_running

    def start(self) -> None:
        self.proc = FFMPEG(*self.ffparams)
        self.proc.start()

    def stop(self) -> None:
        if not self.is_running:
            return
        self.aborted = True
        self.proc.stop()

    def wait(self, progress_handler) -> None:
        def position_handler(position: float):
            duration = self.asset["duration"]
            if not duration:
                progress_handler(None)
                return
            progress = (position / duration) * 100
            progress_handler(progress)

        self.proc.wait(position_handler)

    def finalize(self) -> None:
        if self.aborted:
            for temp_path in self.files:
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

        if self.proc.return_code > 0:
            nebula.log.error(self.proc.stderr.read())
            raise ConversionError("Encoding failed\n" + self.proc.error_log)

        for temp_path, target_path in self.files.items():
            try:
                nebula.log.debug(f"Moving {temp_path} to {target_path}")
                os.rename(temp_path, target_path)
            except IOError:
                raise ConversionError("Unable to move output file")
