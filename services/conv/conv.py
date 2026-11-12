import time

import nebula
from nebula.base_service import BaseService
from nebula.db import DB
from nebula.enum import JobState
from nebula.jobs import Action, get_job
from nxtools import s2words, xml
from services.conv.common import ConversionAborted
from services.conv.ffmpeg import NebulaFFMPEG
from services.conv.melt import NebulaMelt

FORCE_INFO_EVERY = 20

available_encoders = {
    "ffmpeg": NebulaFFMPEG,
    "melt": NebulaMelt,
}


class Service(BaseService):
    def on_init(self):
        self.service_type = "conv"
        self.actions = []
        db = DB()
        db.query(
            """
            SELECT id, title, service_type, settings
            FROM actions ORDER BY id
            """
        )
        for id_action, title, service_type, settings in db.fetchall():
            if service_type == self.service_type:
                nebula.log.debug(f"Registering action {title}")
                self.actions.append(Action(id_action, title, xml(settings)))
        self.reset_jobs()

    def reset_jobs(self):
        db = DB()
        db.query(
            """
            UPDATE jobs SET
                id_service=NULL,
                progress=0,
                retries=0,
                status=5,
                message='Restarting after service restart',
                start_time=0,
                end_time=0
            WHERE
                id_service=%s AND STATUS IN (0,1,5)
            RETURNING id
            """,
            [self.id_service],
        )
        for (id_job,) in db.fetchall():
            nebula.log.info(f"Restarting job ID {id_job} (converter restarted)")
        db.commit()

    def check_abort(self) -> bool:
        """Asked while encoding: was the job aborted, restarted or taken by
        another service (its status is then in progress again)?"""
        if not self.job:
            return False
        stat = self.job.get_status()
        return self.job.lost or stat in (JobState.RESTART, JobState.ABORTED)

    def job_aborted(self) -> None:
        """The encoder stopped because check_abort() said so."""
        assert self.job
        if self.job.lost:
            # Not ours to update any more
            nebula.log.warning(f"{self.job} is no longer ours, stopped")
        elif self.job.status == JobState.RESTART:
            self.job.restart()
        else:
            self.job.abort()

    def progress_handler(self, progress: float | None = None):
        if not self.job:
            return
        if progress is None:
            message = "Encoding: Unknown progress"
            progress = 0
        else:
            message = f"Encoding: {progress:.02f}%"
        self.job.set_progress(progress, message)

    def on_main(self):
        db = DB()
        self.job = get_job(
            self.id_service, [action.id for action in self.actions], db=db
        )
        if not self.job:
            return
        nebula.log.info(f"Got {self.job}")

        asset = self.job.asset
        action = self.job.action

        try:
            job_params = self.job.settings
        except Exception:
            nebula.log.traceback()
            job_params = {}

        tasks = action.settings.findall("task")
        job_start_time = time.time()

        for id_task, task in enumerate(tasks):
            using = task.attrib.get("mode")
            if using not in available_encoders:
                self.job.fail(
                    f"Unknown encoder {using!r} in task {id_task + 1}", critical=True
                )
                return

            self.encoder = available_encoders[using](asset, task, job_params)

            nebula.log.debug(f"Configuring task {id_task + 1} of {len(tasks)}")
            try:
                try:
                    self.encoder.configure()
                except Exception as e:
                    self.job.fail(f"Failed to configure task {id_task + 1}: {e}")
                    nebula.log.traceback()
                    return

                nebula.log.info(f"Starting task {id_task + 1} of {len(tasks)}")
                try:
                    self.encoder.run(self.progress_handler, self.check_abort)
                except ConversionAborted:
                    self.job_aborted()
                    return
                except Exception as e:
                    self.job.fail(f"Failed to encode task {id_task + 1}: {e}")
                    nebula.log.traceback()
                    return

                nebula.log.debug(f"Finalizing task {id_task + 1} of {len(tasks)}")
                try:
                    self.encoder.finalize()
                except Exception as e:
                    self.job.fail(f"Failed to finalize task {id_task + 1}: {e}")
                    return
            finally:
                # Whatever finalize() didn't move into place
                self.encoder.remove_temp_files()

            job_params = self.encoder.params

        job = self.job  # noqa
        assert job
        assert action

        for success_script in action.settings.findall("success"):
            nebula.log.info("Executing success script")
            success_script = success_script.text
            try:
                exec(success_script)
            except Exception:
                nebula.log.traceback()
                self.job.fail("Failed to execute success script")
                return

        elapsed_time = time.time() - job_start_time
        duration = asset["duration"] or 1
        speed = duration / elapsed_time

        self.job.done(f"Finished in {s2words(elapsed_time)} ({speed:.02f}x realtime)")
