import json
import time
from typing import Any

from nxtools import xml

from nebula.db import DB
from nebula.enum import ContentType, JobState, MediaType, ObjectStatus
from nebula.log import log
from nebula.messaging import messaging
from nebula.objects import Asset

_ = ObjectStatus, ContentType, MediaType, JobState


MAX_RETRIES = 3


class Action:
    def __init__(self, id_action, title, settings):
        self.id = id_action
        self.title = title
        self.settings = settings
        try:
            create_if = settings.findall("create_if")[0]
        except IndexError:
            self.create_if = None
        else:
            if create_if is not None:
                if create_if.text:
                    self.create_if = create_if.text
                else:
                    self.create_if = None

        try:
            start_if = settings.findall("start_if")[0]
        except IndexError:
            self.start_if = None
        else:
            if start_if is not None:
                if start_if.text:
                    self.start_if = start_if.text
                else:
                    self.start_if = None

        try:
            skip_if = settings.findall("skip_if")[0]
        except IndexError:
            self.skip_if = None
        else:
            if skip_if is not None:
                if skip_if.text:
                    self.skip_if = skip_if.text
                else:
                    self.skip_if = None

    @property
    def created_key(self):
        return f"job_created/{self.id}"

    def should_create(self, asset: Asset):
        _ = asset
        if self.create_if:
            return eval(self.create_if)
        return False

    def should_start(self, asset: Asset):
        _ = asset
        if self.start_if:
            return eval(self.start_if)
        return True

    def should_skip(self, asset: Asset):
        _ = asset
        if self.skip_if:
            return eval(self.skip_if)
        return False


class Actions:
    def __init__(self):
        self.data = {}

    def load(self, id_action: int):
        db = DB()
        db.query("SELECT title, settings FROM actions WHERE id = %s", [id_action])
        for title, settings in db.fetchall():
            self.data[id_action] = Action(id_action, title, xml(settings))

    def __getitem__(self, key):
        if key not in self.data:
            self.load(key)
        return self.data.get(key, False)


actions = Actions()


class Job:
    _asset: Asset | None = None
    _settings: dict[str, Any] | None = None
    _action: Action | None = None
    _db: DB | None = None

    id_service: int | None = None
    id_user: int | None = None
    priority: int = 3
    retries: int = 0
    status: JobState = JobState.PENDING

    def __init__(self, id: int, db: DB | None = None):
        self._db = db
        self.id = id
        self.id_service = None
        self.id_user = 0
        self.priority = 3
        self.retries = 0
        self.status = JobState.PENDING

    @property
    def id_asset(self) -> int:
        assert self.asset and self.asset.id
        return self.asset.id

    @property
    def id_action(self) -> int:
        assert self.action and self.action.id
        return self.action.id

    @property
    def db(self) -> DB:
        if not self._db:
            self._db = DB()
        return self._db

    @property
    def asset(self) -> Asset:
        if self._asset is None:
            self.load()
        assert self._asset
        return self._asset

    @property
    def settings(self):
        if self._settings is None:
            self.load()
        return self._settings

    @property
    def action(self):
        if self._action is None:
            self.load()
        return self._action

    def __repr__(self):
        assert self.action
        return f"job ID:{self.id} [{self.action.title}@{self.asset}]"

    def load(self):
        self.db.query(
            """
            SELECT
                id_action,
                id_asset,
                id_service,
                id_user,
                settings,
                priority,
                retries,
                status,
                progress,
                 message
            FROM jobs WHERE id=%s
            """,
            [self.id],
        )
        for (
            id_action,
            id_asset,
            id_service,
            id_user,
            settings,
            priority,
            retries,
            status,
            progress,
            message,
        ) in self.db.fetchall():
            self.id_service = id_service
            self.id_user = id_user
            self.priority = priority
            self.retries = retries
            self.status = status
            self.progress = progress
            self.message = message
            self._settings = settings
            self._asset = Asset(id_asset, db=self.db)
            self._action = actions[id_action]
            return
        log.error(f"No such {self}")

    def take(self, id_service):
        now = time.time()
        self.db.query(
            """
            UPDATE jobs SET
                id_service=%s,
                start_time=%s,
                end_time=NULL,
                status=1,
                progress=0
            WHERE id=%s AND id_service IS NULL
            """,
            [id_service, now, self.id],
        )
        self.db.commit()
        self.db.query(
            "SELECT id FROM jobs WHERE id=%s AND id_service=%s", [self.id, id_service]
        )
        if self.db.fetchall():
            messaging.send(
                "job_progress",
                id=self.id,
                id_asset=self.id_asset,
                id_action=self.id_action,
                stime=now,
                status=1,
                progress=0,
                message="Starting...",
            )
            return True
        return False

    def set_progress(self, progress, message="In progress"):
        db = DB()
        progress = round(progress, 2)
        db.query(
            """
            UPDATE jobs SET
                status=1,
                progress=%s,
                message=%s
            WHERE id=%s
            """,
            [progress, message, self.id],
        )
        db.commit()
        messaging.send(
            "job_progress",
            id=self.id,
            id_asset=self.id_asset,
            id_action=self.id_action,
            status=JobState.IN_PROGRESS,
            progress=progress,
            message=message,
        )

    def get_status(self):
        self.db.query("SELECT status FROM jobs WHERE id=%s", [self.id])
        try:
            self.status = self.db.fetchall()[0][0]
        except IndexError:
            log.error(f"No such {self}")
            return 0
        return self.status

    def abort(self, message="Aborted"):
        now = time.time()
        log.warning(f"{self} aborted")
        self.db.query(
            """
            UPDATE jobs SET
                end_time=%s,
                status=4,
                message=%s,
                progress=0
            WHERE id=%s
            """,
            [now, message, self.id],
        )
        self.db.commit()
        self.status = JobState.ABORTED
        messaging.send(
            "job_progress",
            id=self.id,
            id_asset=self.id_asset,
            id_action=self.id_action,
            etime=now,
            status=JobState.ABORTED,
            progress=0,
            message=message,
        )

    def restart(self, message="Restarted"):
        log.warning(f"{self} restarted")
        self.db.query(
            """
            UPDATE jobs SET
                id_service=NULL,
                start_time=NULL,
                end_time=NULL,
                status=5,
                retries=0,
                progress=0,
                message=%s
            WHERE id=%s
            """,
            [message, self.id],
        )
        self.db.commit()
        self.status = JobState.RESTART
        messaging.send(
            "job_progress",
            id=self.id,
            id_asset=self.id_asset,
            id_action=self.id_action,
            stime=None,
            etime=None,
            status=5,
            progress=0,
            message=message,
        )

    def fail(self, message="Failed", critical=False):
        if critical:
            retries = MAX_RETRIES
        else:
            retries = self.retries + 1
        self.db.query(
            """
            UPDATE jobs SET
                id_service=NULL,
                retries=%s,
                priority=%s,
                status=3,
                progress=0,
                message=%s
            WHERE id=%s
            """,
            [retries, max(0, self.priority - 1), message, self.id],
        )
        self.db.commit()
        self.status = JobState.FAILED
        log.error(f"{self}: {message}")
        messaging.send(
            "job_progress",
            id=self.id,
            id_asset=self.id_asset,
            id_action=self.id_action,
            status=JobState.FAILED,
            progress=0,
            message=message,
        )

    def done(self, message="Completed"):
        assert self.action
        now = time.time()
        self.db.query(
            """
            UPDATE jobs SET
                status=2,
                progress=100,
                end_time=%s,
                message=%s
            WHERE id=%s
            """,
            [now, message, self.id],
        )
        self.db.commit()
        self.status = JobState.COMPLETED
        log.success(f"{self}: {message}")
        messaging.send(
            "job_progress",
            id=self.id,
            id_asset=self.asset.id,
            id_action=self.action.id,
            status=JobState.COMPLETED,
            etime=now,
            progress=100,
            message=message,
        )


def get_job(id_service: int, action_ids: list[int], db: DB | None = None):
    assert isinstance(action_ids, list), "action_ids must be list of integers"
    if not action_ids:
        return False
    if db is None:
        db = DB()
    now = time.time()

    running_jobs_count = {}
    db.query(
        """
        SELECT id_action, COUNT(id)
        FROM jobs
        WHERE status=1
        GROUP by id_action
        """
    )
    for id_action, cnt in db.fetchall():
        running_jobs_count[id_action] = cnt

    q = """
        SELECT
            id,
            id_action,
            id_asset,
            id_user,
            settings,
            priority,
            retries,
            status
        FROM jobs
        WHERE
            status IN (0,3,5)
            AND id_action IN %s
            AND id_service IS NULL
            AND retries < %s
            ORDER BY priority DESC, creation_time DESC
        """
    db.query(q, [tuple(action_ids), MAX_RETRIES])

    for (
        id_job,
        id_action,
        id_asset,
        id_user,
        settings,
        priority,
        retries,
        status,
    ) in db.fetchall():
        asset = Asset(id_asset, db=db)
        action = actions[id_action]
        job = Job(id_job, db=db)
        job._asset = asset
        job._settings = settings
        job.priority = priority
        job.retries = retries
        job.id_user = id_user

        #
        # Limit max running jobs
        #
        # This is used for example for playout jobs - multiple
        # running jobs at once may cause storage performance issues
        # and dropped frames
        #

        max_running_jobs = action.settings.attrib.get("max_jobs", 0)
        try:
            max_running_jobs = int(max_running_jobs)
        except ValueError:
            max_running_jobs = 0
        if max_running_jobs:
            running_jobs = running_jobs_count.get(id_action, 0)
            if running_jobs >= max_running_jobs:
                continue  # Maximum allowed jobs already running. skip

        #
        # Limit using run_on whitelist
        #
        # This is used to allow action to run only on specific services
        # (for example, only on services running on hosts with specific hardware)
        #
        # Usage:
        # Add `run_on` tag to action settings with comma-separated list of
        # service IDs. For example:
        #
        # <run_on>1,2,3</run_on>
        #

        run_on_services: list[int] = []
        for run_on_tag in action.settings.findall("run_on"):
            try:
                value = [int(r.strip()) for r in run_on_tag.text.split(",")]
            except ValueError:
                log.error(
                    f"Invalid run_on value for action {action}: {run_on_tag.text}"
                )
                continue
            run_on_services.extend(value)

        if run_on_services and (id_service not in run_on_services):
            continue

        #
        # Pre-script filtering
        #

        for pre in action.settings.findall("pre"):
            if pre.text:
                try:
                    exec(pre.text)
                except Exception:
                    log.traceback()
                    continue
        if not action:
            log.warning(f"Unable to get job. No such action ID {id_action}")
            continue

        if status != 5 and action.should_skip(asset):
            log.info(f"Skipping {job}")
            db.query(
                """
                UPDATE jobs SET
                    status=6,
                    message='Skipped',
                    start_time=%s,
                    end_time=%s
                WHERE id=%s
                """,
                [now, now, id_job],
            )
            db.commit()
            continue

        if action.should_start(asset):
            if job.take(id_service):
                return job
            else:
                log.warning(f"Unable to take {job}")
                continue
        else:
            db.query("UPDATE jobs SET message='Waiting' WHERE id=%s", [id_job])
            messaging.send(
                "job_progress",
                id=id_job,
                id_asset=id_asset,
                id_action=id_action,
                status=status,
                progress=0,
                message="Waiting",
            )
            db.commit()
    return False


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
