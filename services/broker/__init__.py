import time

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import ObjectStatus
from nebula.jobs import Action, send_to
from nebula.objects import Asset
from nxtools import xml

# Margin for clock skew between hosts and the integer mtime column
MTIME_MARGIN = 5


class Service(BaseService):
    def on_init(self) -> None:
        self.actions = []
        db = DB()
        # import actions are started by the import service, not the broker
        db.query(
            """
            SELECT id, title, settings FROM actions
            WHERE service_type NOT IN ('import')
            """
        )
        for id, title, settings in db.fetchall():
            settings = xml(settings)
            self.actions.append(Action(id, title, settings))
        self.created_keys = [a.created_key for a in self.actions if a.create_if]

        # Between full scans, only assets changed since the previous pass are
        # checked. Full scans catch time-based conditions.
        settings = getattr(self, "settings", None)  # None without settings XML
        attrib = settings.attrib if settings is not None else {}
        self.full_scan_interval = int(attrib.get("full_scan_interval", 300))
        self.last_full_scan = 0.0
        self.last_pass = 0.0

    def on_main(self):
        if not self.created_keys:
            return
        start = time.time()
        full_scan = start - self.last_full_scan >= self.full_scan_interval

        # Skip assets for which every action has already been created
        query = """
            SELECT meta FROM assets
            WHERE status = %s AND NOT (meta ?& %s)
        """
        args: list = [ObjectStatus.ONLINE, self.created_keys]
        if not full_scan:
            query += " AND mtime >= %s"
            args.append(int(self.last_pass) - MTIME_MARGIN)

        db = DB()
        db.query(query, args)
        rows = db.fetchall()
        for (meta,) in rows:
            self.proc(Asset(meta=meta, db=db))

        self.last_pass = start
        if full_scan:
            self.last_full_scan = start
            nebula.log.debug(
                f"Checked {len(rows)} assets in {time.time() - start:.2f} s"
            )

    def proc(self, asset: Asset) -> None:
        for action in self.actions:
            assert asset.id is not None

            if action.created_key in asset.meta:
                continue

            if action.should_create(asset):
                nebula.log.info(f"{asset} matches action condition {action.title}")
                try:
                    _ = send_to(
                        asset.id,
                        action.id,
                        restart_existing=False,
                        restart_running=False,
                        db=asset.db,
                    )
                except Exception as e:
                    nebula.log.error(f"Failed to send {asset} to {action.title}: {e}")

                asset[action.created_key] = 1
                asset.save(set_mtime=False)
