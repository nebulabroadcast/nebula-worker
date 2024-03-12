from nxtools import xml

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import ObjectStatus
from nebula.jobs import Action, send_to
from nebula.objects import Asset


class Service(BaseService):
    def on_init(self):
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

    def on_main(self):
        db = DB()
        db.query("SELECT id, meta FROM assets WHERE status=%s", [ObjectStatus.ONLINE])
        for _, meta in db.fetchall():
            asset = Asset(meta=meta, db=db)
            self.proc(asset)

    def proc(self, asset):
        for action in self.actions:
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
