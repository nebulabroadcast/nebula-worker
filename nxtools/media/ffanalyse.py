__all__ = ["FFAnalyse", "ffanalyse"]

import re
import signal
import subprocess

from nxtools.common import PLATFORM
from nxtools.logging import logging


class FFAnalyse:
    def __init__(self, input_path, **kwargs):
        self.proc = None
        self.cmd = ["ffmpeg", "-y"]
        if kwargs.get("start", False):
            self.cmd.extend(["-ss", str(kwargs["start"])])
        self.cmd.extend(["-i", input_path])
        if kwargs.get("duration", False):
            self.cmd.extend(["-t", str(kwargs["duration"])])

        if kwargs.get("audio", True):
            afilters = "silencedetect=n=-20dB:d=5,ebur128,volumedetect"
            self.cmd.extend(self.make_profile(["filter:a", afilters]))
        else:
            self.cmd.extend(self.make_profile("an"))

        if kwargs.get("video", True):
            vfilters = "idet"
            self.cmd.extend(self.make_profile(["filter:v", vfilters]))
        else:
            self.cmd.extend(self.make_profile("vn"))

        self.cmd.extend(self.make_profile(["f", "null"]))
        self.cmd.append("-")
        self.debug = kwargs.get("debug", False)
        self.reset_stderr()

    def reset_stderr(self):
        self.buff = b""
        self.error_log = ""

    @staticmethod
    def make_profile(p):
        cmd = []
        if type(p) == list and len(p) == 2:
            key, val = p
        elif type(p) == list:
            key = p[0]
            val = False
        else:
            key = p
            val = False
        cmd.append("-" + str(key))
        if val:
            cmd.append(str(val))
        return cmd

    @property
    def is_running(self):
        return bool(self.proc) and self.proc.poll() is None

    @property
    def stdin(self):
        return self.proc.stdin

    @property
    def stderr(self):
        return self.proc.stderr

    @property
    def return_code(self):
        return self.proc.returncode

    def stop(self):
        if not self.proc:
            return False
        if PLATFORM == "windows":
            self.proc.send_signel(signal.CTRL_C_EVENT)
        else:
            self.proc.send_signal(signal.SIGINT)
        self.proc.wait()
        return True

    def work(self, progress_handler=None):
        self.reset_stderr()
        result = {}
        tags = [
            ("mean_volume:", "gain/mean"),
            ("max_volume:", "gain/peak"),
            ("I:", "r128/i"),
            ("Threshold:", "r128/t"),
            ("LRA:", "r128/lra"),
            ("Threshold:", "r128/lra/t"),
            ("LRA low:", "r128/lra/l"),
            ("LRA high:", "r128/lra/r"),
        ]
        logging.debug("Executing", " ".join(self.cmd))
        self.proc = subprocess.Popen(self.cmd, stderr=subprocess.PIPE)
        silences = []
        buff = b""
        exp_tag = tags.pop(0)
        last_idet = ""
        while True:
            ch = self.proc.stderr.read(1)
            if not ch:
                break
            if ch in [b"\r", b"\n"]:
                line = buff.decode("utf-8").strip()

                if line.startswith("[Parsed_ebur128"):
                    pass

                elif line.startswith("frame="):
                    m = re.match(r".*frame=\s*(\d+)\s*fps.*", line)
                    if m and progress_handler:
                        progress_handler(int(m.group(1)))

                elif line.find("silence_end") > -1:
                    m = re.match(
                        r".*silence_end:\s*(\d+\.?\d*).*silence_duration:\s*(\d+\.?\d*).*",
                        line,
                    )
                    if m:
                        e = float(m.group(1))
                        s = max(0, e - float(m.group(2)))
                        silences.append([s, e])

                elif line.find("Repeated Fields") > -1:
                    last_idet = line

                elif line.find(exp_tag[0]) > -1:
                    value = float(line.split()[-2])
                    result[exp_tag[1]] = value
                    try:
                        exp_tag = tags.pop(0)
                    except IndexError:
                        break
                else:
                    self.error_log += line + "\n"
                if self.debug:
                    print(line.rstrip())
                buff = b""
            else:
                buff += ch

        if silences:
            result["silence"] = silences

        if last_idet:
            exp = r".*Repeated Fields: Neither:\s*(\d+)\s*Top:\s*(\d+)\s*Bottom:\s*(\d+).*"
            m = re.match(exp, last_idet)
            if m:
                n = int(m.group(1))
                t = int(m.group(2))
                b = int(m.group(3))
                tot = n + t + b
                if n / float(tot) < 0.9:
                    result["is_interlaced"] = True
        return result


def ffanalyse(input_path, **kwargs):
    ff = FFAnalyse(input_path, **kwargs)
    return ff.work(progress_handler=kwargs.get("progress_handler", None))
