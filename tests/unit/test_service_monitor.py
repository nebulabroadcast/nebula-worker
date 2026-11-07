import os
import subprocess
import time
from types import SimpleNamespace

from dispatch.service_monitor import ServiceMonitor


def group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


def test_kill_service_stops_children() -> None:
    # Like a service running ffmpeg: a process with a child, own process group
    proc = subprocess.Popen(["sh", "-c", "sleep 60 & wait"], start_new_session=True)
    monitor = SimpleNamespace(services={7: [proc, "test"]})
    time.sleep(0.2)
    ServiceMonitor.kill_service(monitor, 7)  # type: ignore[arg-type]
    proc.wait(timeout=5)
    for _ in range(50):  # the orphaned child is reaped by init
        if not group_alive(proc.pid):
            break
        time.sleep(0.1)
    assert not group_alive(proc.pid), "child survived"


def test_kill_finished_or_unknown_service() -> None:
    proc = subprocess.Popen(["true"], start_new_session=True)
    proc.wait()
    monitor = SimpleNamespace(services={7: [proc, "test"]})
    ServiceMonitor.kill_service(monitor, 7)  # type: ignore[arg-type]
    ServiceMonitor.kill_service(monitor, 8)  # type: ignore[arg-type]
