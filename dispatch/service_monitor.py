import os
import socket
import subprocess
import time

import nebula
from dispatch.agents import BaseAgent
from nebula.enum import ServiceState

HOSTNAME = socket.gethostname()


class ServiceMonitor(BaseAgent):
    def on_init(self):
        self.services = {}
        db = nebula.DB()
        db.query("SELECT id, pid FROM services WHERE host=%s", [HOSTNAME])
        for _, pid in db.fetchall():
            if pid:
                self.kill_service(pid)
        db.query("UPDATE services SET state = 0 WHERE host=%s", [HOSTNAME])
        db.commit()

    def on_shutdown(self):
        services = self.services.keys()
        for id_service in services:
            self.kill_service(id_service=id_service)

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
                    self.kill_service(self.services[id][0].pid)

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
            db.query("UPDATE services SET state=0 WHERE id = %s", [id_service])
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

        self.services[id_service] = [
            subprocess.Popen(proc_cmd, cwd=nebula.config.nebula_root),
            title,
        ]

    def stop_service(self, id_service, title, db=False):
        nebula.log.info(f"Stopping service ID {id_service} ({title})")

    def kill_service(self, pid=False, id_service=False):
        if id_service in self.services:
            pid = self.services[id_service][0].pid
        if pid == os.getpid() or pid == 0:
            return
        nebula.log.info(f"Attempting to kill PID {pid}")
        os.system(
            os.path.join(
                nebula.config.nebula_root,
                "support",
                f"kill_tree.sh {pid}",
            )
        )
