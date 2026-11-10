import functools
import os
import subprocess
from typing import Any

import jinja2

import nebula
from nebula.ffmpeg import AbortWatcher
from nebula.storages import storages

from .common import BaseEncoder, ConversionAborted, ConversionError, temp_file


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

        # Shared namespace for task scripts and expressions, so variables
        # defined in <script> are visible to subsequent evals.
        # (exec() no longer writes to function locals since Python 3.13)
        ns = {**globals(), "self": self, "asset": asset, "params": params}

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
            ns["p"] = p
            if p.tag == "param":
                key = p.attrib["name"]
                value = str(eval(p.text, ns)) if p.text else ""
                enc_params.append(f"{key}={value}")

            elif p.tag == "script":
                if p.text:
                    try:
                        exec(p.text, ns)
                    except Exception:
                        nebula.log.traceback()

            elif p.tag == "paramset" and eval(p.attrib["condition"], ns):
                for pp in p.findall("param"):
                    key = pp.attrib["name"]
                    value = str(eval(pp.text, ns)) if pp.text else ""
                    enc_params.append(f"{key}={value}")

            elif p.tag == "output":
                id_storage = int(eval(p.attrib["storage"], ns))
                storage = storages[id_storage]
                if not storage.is_writable:
                    raise ConversionError("Target storage is not writable")

                if not p.text:
                    raise ConversionError("Output path is not specified")

                target_rel_path = eval(p.text, ns)
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

    def run(self, progress_handler, check_abort=None) -> None:
        nebula.log.debug(f"Executing: {' '.join(self.cmd)}")
        proc = subprocess.Popen(
            self.cmd,
            stderr=subprocess.PIPE,
            stdout=None,
            universal_newlines=True,
        )
        assert proc.stderr
        buff = ""
        current_percent = 0
        try:
            with AbortWatcher(proc, check_abort) as watcher:
                while proc.poll() is None:
                    buff += proc.stderr.read(1)
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
                proc.wait()
        except BaseException:
            # Don't leave melt running if reporting progress fails
            proc.kill()
            proc.wait()
            raise

        if watcher.aborted:
            raise ConversionAborted()
        # != 0: a process killed by a signal has a negative code
        if proc.returncode != 0:
            nebula.log.error(proc.stderr.read())
            raise ConversionError("Encoding failed")
