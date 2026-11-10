import os
import signal
import socket
import subprocess
import time

import nebula
from dispatch.agents import BaseAgent
from nebula.enum import ServiceState

HOSTNAME = socket.gethostname()


def is_service_process(pid: int, id_service: int) -> bool:
    """Whether PID is still `manage run <id_service>` (PIDs are reused)."""
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            argv = f.read().split(b"\0")
    except OSError:
        return False
    run = [b"run", str(id_service).encode()]
    return any(arg.endswith(b"manage") for arg in argv) and any(
        argv[i : i + 2] == run for i in range(len(argv))
    )


def kill_stale_service(id_service: int, pid: int) -> None:
    """Stop a service a previous dispatch left running (outside a container,
    where dispatch can restart without its children)."""
    if pid == os.getpid() or not is_service_process(pid, id_service):
        return
    nebula.log.warning(f"Killing service ID {id_service} left running, PID {pid}")
    try:
        os.killpg(pid, signal.SIGTERM)  # its own group: start_new_session
    except (ProcessLookupError, PermissionError):
        pass


class ServiceMonitor(BaseAgent):
    def on_init(self):
        self.services = {}
        db = nebula.DB()
        db.query("SELECT id, pid FROM services WHERE host=%s AND pid > 0", [HOSTNAME])
        for id_service, pid in db.fetchall():
            kill_stale_service(id_service, pid)
        db.query("UPDATE services SET state = 0, pid = 0 WHERE host=%s", [HOSTNAME])
        db.commit()

    def on_shutdown(self):
        services = self.services.keys()
        for id_service in services:
            self.kill_service(id_service)

    @property
    def running_services(self):
        result = []
        for id_service in self.services.keys():
            proc, title = self.services[id_service]
            if proc.poll() is None:
                result.append((id_service, title))
        return result

    def main(self):
        db = nebula.DB()
        db.query(
            """
            SELECT
                id,
                title,
                autostart,
                state,
                last_seen
            FROM services
            WHERE host=%s
            """,
            [HOSTNAME],
        )

        #
        # Start / stop service
        #

        for id, title, autostart, state, last_seen in db.fetchall():
            nebula.msg(
                "service_state",
                id=id,
                state=state,
                autostart=autostart,
                last_seen=last_seen,
                last_seen_before=max(0, int(time.time() - last_seen)),
            )
            if state == ServiceState.STARTING:  # Start service
                if id not in self.services.keys():
                    self.start_service(id, title, db=db)

            elif state == ServiceState.KILL:  # Kill service
                if id in self.services.keys():
                    self.kill_service(id)

        #
        # Real service state
        #

        service_list = list(self.services.keys())
        for id_service in service_list:
            proc, title = self.services[id_service]
            if proc.poll() is None:
                continue
            del self.services[id_service]
            nebula.log.warning(f"Service ID {id_service} ({title}) terminated")
            # Its ffmpeg etc. may outlive it; the group exists while they run
            try:
                os.killpg(proc.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            db.query("UPDATE services SET state=0, pid=0 WHERE id = %s", [id_service])
            db.commit()

        #
        # Autostart
        #

        db.query(
            """
            SELECT id, title
            FROM services
            WHERE host=%s AND state=0 AND autostart=true
            """,
            [HOSTNAME],
        )
        for id, title in db.fetchall():
            if id not in self.services.keys():
                nebula.log.debug(f"AutoStarting service ID {id} ({title})")
                self.start_service(id, title)

    def start_service(self, id_service, title, db=False):
        proc_cmd = [
            os.path.join(nebula.config.nebula_root, "manage"),
            "run",
            str(id_service),
            f'"{title}"',
        ]

        nebula.log.info(f"Starting service ID {id_service} ({title})")

        # Own process group, so killpg also stops its ffmpeg etc.
        proc = subprocess.Popen(
            proc_cmd, cwd=nebula.config.nebula_root, start_new_session=True
        )
        self.services[id_service] = [proc, title]
        # So a restarted dispatch can find it (kill_stale_service)
        pid_db = nebula.DB()
        pid_db.query(
            "UPDATE services SET pid = %s WHERE id = %s", [proc.pid, id_service]
        )
        pid_db.commit()

    def stop_service(self, id_service, title, db=False):
        nebula.log.info(f"Stopping service ID {id_service} ({title})")

    def kill_service(self, id_service: int) -> None:
        proc, title = self.services.get(id_service, (None, None))
        if proc is None:
            return
        # Also when the service itself has exited: its ffmpeg may still run
        if proc.poll() is None:
            nebula.log.info(f"Killing service ID {id_service} ({title})")
        try:
            os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
