import json
import time
from typing import Any

from nebula.db import DB
from nebula.log import log
from nebula.messaging import messaging


def send_to(
    id_asset: int,
    id_action: int,
    id_service: int | None = None,
    settings: dict[str, Any] | None = None,
    id_user: int | None = None,
    priority: int = 3,
    restart_existing: bool = True,
    restart_running: bool = False,
    db: DB | None = None,
) -> int:
    if db is None:
        db = DB()

    assert id_asset, "You must specify an existing object"

    if settings is None:
        settings = {}

    db.query(
        """
        SELECT id
        FROM jobs
        WHERE id_asset=%s AND id_action=%s AND settings=%s
        """,
        [id_asset, id_action, json.dumps(settings)],
    )
    res = db.fetchall()
    if res:
        if restart_existing:
            conds = "0,5"
            if not restart_running:
                conds += ",1"

            db.query(
                f"""
                UPDATE jobs SET
                    id_user=%s,
                    id_service=%s,
                    message='Restart requested',
                    status=5,
                    retries=0,
                    creation_time=%s,
                    start_time=NULL,
                    end_time=NULL
                WHERE id=%s
                    AND status NOT IN ({conds})
                RETURNING id
                """,
                [id_user, id_service, time.time(), res[0][0]],
            )
            db.commit()
            if db.fetchall():
                messaging.send(
                    "job_progress",
                    id=res[0][0],
                    id_asset=id_asset,
                    id_action=id_action,
                    progress=0,
                )
                log.trace(f"Restarted job {res[0][0]}")
                return res[0][0]
            log.trace(f"Job {res[0][0]} is running. Not restarting")
            return res[0][0]

        else:
            log.trace(f"Job {res[0][0]} exists. Not restarting")
            return res[0][0]

    #
    # Create a new job
    #

    db.query(
        """INSERT INTO jobs (
            id_asset,
            id_action,
            id_user,
            id_service,
            settings,
            priority,
            message,
            creation_time
        ) VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s,
            'Pending',
            %s
        )
        RETURNING id
        """,
        [
            id_asset,
            id_action,
            id_user,
            id_service,
            json.dumps(settings),
            priority,
            time.time(),
        ],
    )

    try:
        id_job = db.fetchall()[0][0]
        db.commit()
    except Exception as e:
        log.traceback()
        raise Exception("Unable to create job") from e

    messaging.send(
        "job_progress",
        id=id_job,
        id_asset=id_asset,
        id_action=id_action,
        progress=0,
        message="Job created",
    )
    return id_job
