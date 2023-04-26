import sys
import time
from typing import TYPE_CHECKING

from nebula.db import DB
from nebula.enum import ServiceState
from nebula.log import log

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element


class BaseService:
    id_service: int
    settings: "Element"

    def __init__(self, id_service, settings=None):
        log.debug(f"Initializing service ID {id_service}")
        self.id_service = id_service
        self.settings = settings

        try:
            self.on_init()
        except SystemExit:
            sys.exit(1)
        except Exception:
            log.traceback(f"Unable to initialize service ID {id_service}")
            self.shutdown()
        else:
            db = DB()
            db.query(
                "UPDATE services SET last_seen = %s, state=1 WHERE id=%s",
                [time.time(), self.id_service],
            )
            db.commit()
        log.success("Service started")

    @property
    def id(self) -> int:
        return self.id_service

    def on_init(self):
        pass

    def on_main(self):
        pass

    def on_shutdown(self):
        pass

    def soft_stop(self):
        log.info("Graceful shutdown requested")
        db = DB()
        db.query("UPDATE services SET state=3 WHERE id=%s", [self.id_service])
        db.commit()

    def shutdown(self, no_restart=False):
        log.info("Shutting down")
        if no_restart:
            db = DB()
            db.query(
                "UPDATE services SET autostart=FALSE WHERE id=%s", [self.id_service]
            )
            db.commit()
        self.on_shutdown()
        sys.exit(0)

    def heartbeat(self):
        db = DB()
        db.query("SELECT state FROM services WHERE id=%s", [self.id_service])
        try:
            state = db.fetchall()[0][0]
        except IndexError:
            state = ServiceState.KILL
        else:
            if state == 0:
                state = 1
            db.query(
                "UPDATE services SET last_seen=%s, state=%s WHERE id=%s",
                [time.time(), state, self.id_service],
            )
            db.commit()

        if state in [ServiceState.STOPPED, ServiceState.STOPPING, ServiceState.KILL]:
            self.shutdown()
