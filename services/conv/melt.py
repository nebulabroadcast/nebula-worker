import os
import functools
import jinja2
import signal
import subprocess

from typing import Any

import nebula

from nebula.storages import storages


from .common import temp_file, ConversionError


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
    for profile in proc.stdout:
        if profile.startswith(b"  - "):
            result.append(profile[4:].decode().strip())
    proc.wait()
    return result


class NebulaMelt:
    def __init__(self, asset: nebula.Asset, task, params: dict[str, Any]):
        self.asset = asset
        self.task = task
        self.params = params
        self.proc = None
        self.progress = 0
        self.message = "Started"

    def configure(self):
        asset = self.asset
        params = self.params
        assert asset
        assert params is not None

        # TODO: load this from config
        postproc_context: dict[str, Any] | None = None
        profile = "atsc_1080i_50"

        self.files = {}
        self.cmd = ["melt", "-progress"]

        source_path = os.path.join(storages[asset.id_storage].local_path, asset.path)

        if postproc_context is not None:
            with open(self.temp_path, "w") as f:
                f.write(process_template(source_path, postproc_context))
            self.cmd.append(self.temp_path)
        else:
            self.cmd.append(source_path)

        if profile is not None:
            self.cmd.extend(["-profile", self.profile])

        # self.cmd.extend(["-consumer", f"avformat:{target_path}"])

        for p in self.task:
            if p.tag == "param":
                value = str(eval(p.text)) if p.text else ""
                if p.attrib["name"] == "ss":
                    self.cmd.insert(1, "-ss")
                    self.cmd.insert(2, value)
                else:
                    self.cmd.append("-" + p.attrib["name"])
                    if value:
                        self.cmd.append(value)

            elif p.tag == "script":
                if p.text:
                    try:
                        exec(p.text)
                    except Exception:
                        nebula.log.traceback()

            elif p.tag == "paramset" and eval(p.attrib["condition"]):
                for pp in p.findall("param"):
                    value = str(eval(pp.text)) if pp.text else ""
                    self.cmd.append("-" + pp.attrib["name"])
                    if value:
                        self.cmd.append(value)

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
                        )  # noqa)

                if not p.attrib.get("direct", False):
                    self.files[temp_path] = target_path
                    self.cmd.append(temp_path)
                else:
                    self.cmd.append(target_path)

    @property
    def is_running(self) -> bool:
        return bool(self.proc and self.proc.is_running)

    def start(self) -> None:
        self.proc = subprocess.Popen(
            self.cmd,
            stderr=None,
            stdout=subprocess.PIPE,
            universal_newlines=True,
        )

    def stop(self):
        if not self.is_running:
            return None
        self.proc.send_signal(signal.SIGINT)

    def wait(self, progress_handler):
        buff = b""
        while self.proc.poll() is None:
            buff += self.proc.stderr.read(1)
            if buff.endswith(b"\r"):
                line = buff.decode().strip()
                if line.startswith("Current"):
                    progress = line.split(":")[-1].strip()
                    if not progress.isdigit():
                        continue
                    progress = int(progress)
                    print(f"Progress: {progress}%", end="\r")
                else:
                    print(line)
                buff = b""
        self.proc.wait()

    def finalize(self):
        if self.proc.return_code > 0:
            nebula.log.error(self.proc.stderr.read())
            raise ConversionError("Encoding failed")

        for temp_path in self.files:
            target_path = self.files[temp_path]
            try:
                nebula.log.debug(f"Moving {temp_path} to {target_path}")
                os.rename(temp_path, target_path)
            except IOError:
                raise ConversionError("Unable to move output file")
