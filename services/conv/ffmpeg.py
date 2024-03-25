import os
import re
import signal
import subprocess

import nebula
from nebula.storages import storages

from .common import BaseEncoder, ConversionError, temp_file

re_position = re.compile(r"time=(\d{2}):(\d{2}):(\d{2})\.(\d{2})\d*", re.U | re.I)


def time2sec(search):
    hh, mm, ss, cs = search.group(1), search.group(2), search.group(3), search.group(4)
    return int(hh) * 3600 + int(mm) * 60 + int(ss) + int(cs) / 100.0


class NebulaFFMPEG(BaseEncoder):
    def configure(self) -> None:
        self.proc = None
        self.files = {}
        self.ffparams = ["-y"]
        self.ffparams.extend(["-i", self.asset.file_path])
        asset = self.asset
        params = self.params
        assert asset
        assert params is not None
        self.error_log = ""

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
                    except Exception as e:
                        nebula.log.traceback()
                        raise ConversionError("Error in task 'pre' script.") from e

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
        if not self.proc:
            return False
        return self.proc.poll() is None

    def start(self) -> None:
        cmd = ["ffmpeg", "-hide_banner"]
        cmd.extend(str(arg) for arg in self.ffparams)
        nebula.log.info(f"Executing {' '.join(cmd)}")
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def stop(self) -> None:
        if self.proc and self.proc.poll() is None:
            self.aborted = True
            self.proc.send_signal(signal.SIGINT)

    def wait(self, progress_handler) -> None:
        assert self.proc
        assert self.proc.stderr

        duration = self.asset["duration"]
        buff = b""

        while True:
            ch = self.proc.stderr.read(1)
            if not ch:
                break
            if ch in [b"\n", b"\r"]:
                line = buff.decode("utf-8", errors="ignore").strip()

                position_match = re_position.search(line)
                if position_match:
                    position = time2sec(position_match)
                    if not duration:
                        progress_handler(None)
                    else:
                        progress = (position / duration) * 100
                        progress_handler(progress)
                    self.error_log = ""

                elif line == "Press [q] to stop, [?] for help":
                    self.error_log = ""

                else:
                    self.error_log += line + "\n"
                buff = b""
            else:
                buff += ch

        if self.proc:
            self.proc.wait()
            if self.proc.stderr:
                self.error_log += self.proc.stderr.read().decode(
                    "utf-8", errors="ignore"
                )

    def finalize(self) -> None:
        if not self.proc:
            return
        assert self.proc.stderr

        if self.aborted:
            for temp_path in self.files:
                try:
                    os.remove(temp_path)
                except Exception:
                    pass

        if self.proc.returncode > 0:
            nebula.log.error(self.proc.stderr.read())
            raise ConversionError("Encoding failed")

        for temp_path, target_path in self.files.items():
            try:
                nebula.log.debug(f"Moving {temp_path} to {target_path}")
                os.rename(temp_path, target_path)
            except OSError as e:
                raise ConversionError("Unable to move output file") from e
