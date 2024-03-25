import functools
import os
import signal
import subprocess
from typing import Any, Callable

import jinja2

import nebula
from nebula.storages import storages

from .common import BaseEncoder, ConversionError, temp_file


def process_template(source_path: str, context: dict[str, Any] | None = None) -> str:
    if context is None:
        context = {}
    raw_template = open(source_path).read()
    env = jinja2.Environment()
    template = env.from_string(raw_template)
    return template.render(**context)


@functools.cache
def profiles() -> list[str]:
    result = []
    proc = subprocess.Popen(["melt", "-query", "profiles"], stdout=subprocess.PIPE)
    assert proc.stdout
    for profile in proc.stdout:
        if profile.startswith(b"  - "):
            result.append(profile[4:].decode().strip())
    proc.wait()
    return result


class NebulaMelt(BaseEncoder):
    def configure(self):
        asset = self.asset
        params = self.params
        assert asset
        assert params is not None

        self.files = {}
        self.cmd = ["melt", "-progress"]

        source_path = os.path.join(
            storages[asset["id_storage"]].local_path,
            asset["path"],
        )

        # TODO: load this from config
        # postproc_context: dict[str, Any] | None = None
        # if postproc_context is not None:
        #     with open(self.temp_path, "w") as f:
        #         f.write(process_template(source_path, postproc_context))
        #     self.cmd.append(self.temp_path)
        # else:
        self.cmd.append(source_path)

        profile = self.task.attrib.get("profile", None)
        if profile is not None:
            self.cmd.extend(["-profile", profile])

        enc_params: list[str] = []
        for p in self.task:
            if p.tag == "param":
                key = p.attrib["name"]
                value = str(eval(p.text)) if p.text else ""
                enc_params.append(f"{key}={value}")

            elif p.tag == "script":
                if p.text:
                    try:
                        exec(p.text)
                    except Exception:
                        nebula.log.traceback()

            elif p.tag == "paramset" and eval(p.attrib["condition"]):
                for pp in p.findall("param"):
                    key = pp.attrib["name"]
                    value = str(eval(pp.text)) if pp.text else ""
                    enc_params.append(f"{key}={value}")

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

                if p.attrib.get("direct", False):
                    self.cmd.extend(["-consumer", f"avformat:{target_path}"])
                else:
                    self.files[temp_path] = target_path
                    self.cmd.extend(["-consumer", f"avformat:{temp_path}"])
        self.cmd.extend(enc_params)

    @property
    def is_running(self) -> bool:
        return bool(self.proc and self.proc.poll() is None)

    def start(self) -> None:
        nebula.log.debug(f"Executing: {' '.join(self.cmd)}")
        self.proc = subprocess.Popen(
            self.cmd,
            stderr=subprocess.PIPE,
            stdout=None,
            universal_newlines=True,
        )

    def stop(self) -> None:
        if not self.is_running:
            return None
        self.proc.send_signal(signal.SIGINT)  # type: ignore

    def wait(self, progress_handler: Callable) -> None:
        buff = ""
        current_percent = 0
        assert self.proc
        assert self.proc.stderr
        while self.proc.poll() is None:
            buff += self.proc.stderr.read(1)
            if buff.endswith("\r") or buff.endswith("\n"):
                line = buff.strip()
                if line.startswith("Current"):
                    try:
                        progress = int(line.split(":")[-1].strip())
                    except ValueError:
                        pass
                    else:
                        if current_percent != progress:
                            current_percent = progress
                            progress_handler(progress)
                buff = ""
        self.proc.wait()

    def finalize(self) -> None:
        assert self.proc
        assert self.proc.stderr
        if self.proc.returncode > 0:
            nebula.log.error(self.proc.stderr.read())
            raise ConversionError("Encoding failed")

        for temp_path in self.files:
            target_path = self.files[temp_path]
            try:
                nebula.log.debug(f"Moving {temp_path} to {target_path}")
                os.rename(temp_path, target_path)
            except OSError as e:
                raise ConversionError("Unable to move output file") from e
