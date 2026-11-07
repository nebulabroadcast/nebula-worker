import sys
import time
from xml.etree.ElementTree import Element

from nebula.db import DB, OperationalError
from nebula.enum import ServiceState
from nebula.log import log


class BaseService:
    id_service: int
    settings: Element

    def __init__(self, id_service, settings: Element | None = None):
        log.debug(f"Initializing service ID {id_service}")
        self.id_service = id_service
        if settings is not None:
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
        # A failed beat is skipped, not fatal: a database restart must not
        # take the service (and playout) down. The pool reconnects.
        db = DB()
        try:
            # One statement: a stop request set in between can't be overwritten
            db.query(
                """
                UPDATE services SET
                    last_seen = %s,
                    state = CASE WHEN state = 0 THEN 1 ELSE state END
                WHERE id = %s
                RETURNING state
                """,
                [time.time(), self.id_service],
            )
            rows = db.fetchall()
            db.commit()
            state = rows[0][0] if rows else ServiceState.KILL
        except OperationalError as e:
            log.warning(f"Heartbeat failed: {e}")
            return

        if state in [ServiceState.STOPPED, ServiceState.STOPPING, ServiceState.KILL]:
            self.shutdown()
