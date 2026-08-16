import time
from typing import Any

from nebula.db import DB
from nebula.enum import JobState
from nebula.log import log
from nebula.messaging import messaging
from nebula.objects import Asset

from .actions import Action, actions

MAX_RETRIES = 3


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

    def __init__(
        self,
        id: int,
        *,
        db: DB | None = None,
    ):
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

    def take(self, id_service: int) -> bool:
        """Take job for processing by service with id_service.

        This method updates the job's status to "in progress" and assigns it
        to the specified service. Returns True if job was successfully taken
        by service, False otherwise.

        Service should not continue processing the job if this method returns False,
        as it means another service has already taken the job.
        """

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
            "SELECT id FROM jobs WHERE id=%s AND id_service=%s",
            [self.id, id_service],
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

    def set_progress(self, progress: float, message: str | None = None) -> None:
        db = DB()
        progress = round(progress, 2)

        if message is None:
            message = "Job in progress"

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

    def get_status(self) -> JobState:
        self.db.query("SELECT status FROM jobs WHERE id=%s", [self.id])
        try:
            self.status = self.db.fetchall()[0][0]
        except IndexError:
            log.error(f"No such {self}")
            return JobState.PENDING
        return JobState(self.status)

    def abort(self, message: str | None = None) -> None:
        now = time.time()
        if message is None:
            message = "Aborted"
        log.warning(f"{self}: {message}")
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

    def restart(self, message: str | None = None) -> None:
        if message is None:
            message = "Restart requested"
        log.warning(f"{self}: {message}")
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

    def fail(self, message: str | None = None, *, critical: bool = False) -> None:
        if message is None:
            message = "Failed"
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

    def done(self, message: str | None = None) -> None:
        assert self.action
        now = time.time()
        if message is None:
            message = "Completed"
        log.success(f"{self}: {message}")
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


def get_job(
    id_service: int,
    action_ids: list[int],
    *,
    db: DB | None = None,
) -> Job | None:
    assert isinstance(action_ids, list), "action_ids must be list of integers"
    if not action_ids:
        return None
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
            run_on_text = (run_on_tag.text or "").strip()
            if not run_on_text:
                continue
            try:
                value = [int(r.strip()) for r in run_on_text.split(",") if r.isdigit()]
            except ValueError:
                log.error(f"Invalid run_on value for action {action}: {run_on_text}")
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

        if status != JobState.RESTART.value and action.should_skip(asset):
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
            db.query(
                """
                UPDATE jobs SET
                    message='Starting',
                    status=1,
                    progress=0,
                    start_time=%s
                WHERE id=%s
                """,
                [id_job, now],
            )
            messaging.send(
                "job_progress",
                id=id_job,
                id_asset=id_asset,
                id_action=id_action,
                status=status,
                progress=0,
                message="Starting",
            )
            db.commit()
    return None
