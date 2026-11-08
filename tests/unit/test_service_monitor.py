import os
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

import nebula
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


class FakeDB:
    def query(self, *args: object) -> None:
        pass

    def fetchall(self) -> list:
        return []

    def commit(self) -> None:
        pass


def test_children_of_exited_service_are_killed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A service that exits by itself (crash) leaves its ffmpeg running
    monkeypatch.setattr(nebula, "DB", FakeDB)
    proc = subprocess.Popen(["sh", "-c", "sleep 60 & exit 0"], start_new_session=True)
    proc.wait(timeout=5)
    assert group_alive(proc.pid)  # the orphaned sleep
    monitor = SimpleNamespace(services={7: [proc, "test"]})
    ServiceMonitor.main(monitor)  # type: ignore[arg-type]
    assert 7 not in monitor.services
    for _ in range(50):
        if not group_alive(proc.pid):
            break
        time.sleep(0.1)
    assert not group_alive(proc.pid), "orphan survived"


def test_missing_mount_binary_is_a_failed_mount() -> None:
    from dispatch.storage_monitor import exec_mount

    with pytest.raises(RuntimeError):
        exec_mount(["/nonexistent/mount.cifs", "//x/y", "/tmp/z"])


def test_shutdown_kills_children_of_exited_service() -> None:
    # The service exited, but its child (ffmpeg) still runs
    proc = subprocess.Popen(["sh", "-c", "sleep 60 & exit 0"], start_new_session=True)
    proc.wait(timeout=5)
    assert group_alive(proc.pid)
    monitor = SimpleNamespace(services={7: [proc, "test"]})
    monitor.kill_service = lambda id_service: ServiceMonitor.kill_service(
        monitor,  # type: ignore[arg-type]
        id_service,
    )
    ServiceMonitor.on_shutdown(monitor)  # type: ignore[arg-type]
    for _ in range(50):
        if not group_alive(proc.pid):
            break
        time.sleep(0.1)
    assert not group_alive(proc.pid), "child survived"


def fake_service(id_service: int) -> subprocess.Popen:
    # Looks like `manage run <id>` in /proc/<pid>/cmdline
    code = "import time; time.sleep(60)"
    argv = [sys.executable, "-c", code, "/opt/nebula/manage", "run", str(id_service)]
    return subprocess.Popen(argv, start_new_session=True)


def test_stale_service_is_killed() -> None:
    from dispatch.service_monitor import kill_stale_service

    proc = fake_service(7)
    time.sleep(0.2)
    kill_stale_service(7, proc.pid)
    assert proc.wait(timeout=5) != 0


def test_reused_pid_is_not_killed() -> None:
    # The recorded PID now belongs to something else: leave it alone
    from dispatch.service_monitor import is_service_process, kill_stale_service

    other = fake_service(8)
    plain = subprocess.Popen(["sleep", "60"], start_new_session=True)
    try:
        time.sleep(0.2)
        assert not is_service_process(other.pid, 7)
        kill_stale_service(7, other.pid)
        kill_stale_service(7, plain.pid)
        kill_stale_service(7, 999_999_999)  # no such process
        assert other.poll() is None and plain.poll() is None
    finally:
        other.kill()
        plain.kill()
