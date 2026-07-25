import os
import subprocess
import time

import nebula
from dispatch.agents import BaseAgent
from nebula.settings.models import StorageSettings
from nebula.storages import Storage


def exec_mount(cmd: str) -> None:
    proc = subprocess.run(
        cmd,
        capture_output=True,
        shell=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"Mount failed with return code {proc.returncode}"
            f": {proc.stderr.decode().strip()}"
        )


# def handle_nfs_storage(storage: Storage):
#     cmd = f"mount.nfs {storage.path} {storage.local_path}"
#     exec_mount(cmd)


def handle_samba_storage(storage: Storage):
    if time.time() - storage.last_mount_attempt < min(storage.mount_attempts * 5, 120):
        return

    if not os.path.exists(storage.local_path):
        try:
            os.mkdir(storage.local_path)
        except FileExistsError:
            pass
        except Exception:
            nebula.log.traceback(f"Unable to create mountpoint for {storage}")
            storage.last_mount_attempt = time.time()
            storage.mount_attempts = 999
            return

    msg = f"Mounting {storage} (attempt {storage.mount_attempts + 1})..."

    if storage.mount_attempts < 10:
        nebula.log.debug(msg)
    else:
        nebula.log.trace(msg)

    smbopts = []
    for key, value in storage.options.items():
        if key == "login":
            key = "user"
        elif key == "password":
            key = "pass"
        elif key == "samba_version":
            key = "vers"

        if value is None:
            smbopts.append(key)
        else:
            smbopts.append(f"{key}={value}")

    if smbopts:
        opts = f""" -o '{",".join(smbopts)}'"""
    else:
        opts = ""

    cmd = f"mount.cifs {storage.path} {storage.local_path}{opts}"
    if storage.mount_attempts < 5:
        nebula.log.trace(cmd)

    try:
        exec_mount(cmd)
    except RuntimeError as e:
        if storage.mount_attempts < 5:
            nebula.log.error(str(e))
        storage.last_mount_attempt = time.time()
        storage.mount_attempts += 1
    else:
        nebula.log.success(f"{storage} mounted successfully")
        storage.mount_attempts = 0


class StorageMonitor(BaseAgent):
    def on_init(self):
        self.status = {}

    def main(self):
        db = nebula.DB()
        db.query("SELECT id, settings FROM storages")

        for id_storage, storage_settings in db.fetchall():
            storage = Storage(
                StorageSettings(
                    id=id_storage,
                    **storage_settings,
                )
            )

            if not storage.enabled:
                continue

            stats = self.status.get(id_storage, {})
            storage.last_mount_attempt = stats.get("last_mount_attempt", 0)
            storage.mount_attempts = stats.get("mount_attempts", 0)

            if storage.is_mounted:
                continue

            if storage.protocol == "local":
                if not os.path.isdir(storage.path):
                    try:
                        os.makedirs(storage.path)
                    except FileExistsError:
                        pass
                    except Exception:
                        nebula.log.traceback(
                            f"Unable to create mountpoint for {storage}"
                        )
                continue

            if storage.protocol == "samba":
                handle_samba_storage(storage)

            self.status[id_storage] = {
                "last_mount_attempt": storage.last_mount_attempt,
                "mount_attempts": storage.mount_attempts,
            }
