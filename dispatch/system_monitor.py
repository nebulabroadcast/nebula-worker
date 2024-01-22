import socket
import time

import psycopg2

import nebula
from dispatch.agents import BaseAgent

NEBULA_START_TIME = time.time()
HOSTNAME = socket.gethostname()


def update_host_info():
    return

    # mp2id = {}
    # for id_storage, storage in storages.items():
    #     mp2id[storage.local_path] = id_storage
    #
    # p = promexp.Promexp(provider_settings={"casparcg": None})
    # p.metrics.add("runtime_seconds", time.time() - NEBULA_START_TIME)
    #
    # p.collect()
    # for metric in list(p.metrics.data.keys()):
    #     mname, tags = metric
    #     if mname in ["storage_bytes_total", "storage_bytes_free", "storage_usage"]:
    #         id_storage = mp2id.get(tags["mountpoint"])
    #         if id_storage is None:
    #             continue
    #         value = p.metrics.data[metric]
    #         del p.metrics.data[metric]
    #         p.metrics.add(
    #             f"shared_{mname}",
    #             value,
    #             id=id_storage,
    #             title=storages[id_storage].title,
    #         )
    #
    # status = {"metrics": p.metrics.dump()}
    #
    # db = DB()
    # db.query(
    #     "UPDATE hosts SET last_seen=%s, status=%s WHERE hostname=%s",
    #     [time.time(), json.dumps(status), HOSTNAME],
    # )
    # db.commit()


class SystemMonitor(BaseAgent):
    last_update: float = 0

    def on_init(self):
        self.last_update = 0
        db = nebula.DB()
        try:
            db.query(
                """
                INSERT INTO hosts(hostname, last_seen)
                VALUES (%s, %s) ON CONFLICT DO NOTHING
                """,
                [HOSTNAME, time.time()],
            )
        except psycopg2.IntegrityError:
            pass
        else:
            db.commit()

    def on_shutdown(self):
        pass

    def main(self):
        nebula.msg("heartbeat")
        if time.time() - self.last_update > 5:
            try:
                update_host_info()
            except Exception:
                nebula.log.traceback("Unable to update host info")
            self.last_update = time.time()
