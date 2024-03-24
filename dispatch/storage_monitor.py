import os
import subprocess
import time
from typing import Any

import nebula
from dispatch.agents import BaseAgent
from nebula.settings.models import StorageSettings
from nebula.storages import Storage


def exec_mount(cmd: str) -> bool:
    proc = subprocess.Popen(cmd, shell=True)
    while proc.poll() is None:
        time.sleep(0.1)
    if proc.returncode:
        return False
    return True


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

    nebula.log.info(f"{storage} is not mounted. Mounting...")

    smbopts = {}
    if storage.options.get("login"):
        smbopts["user"] = storage.options["login"]
    if storage.options.get("password"):
        smbopts["pass"] = storage.options["password"]
    if storage.options.get("domain"):
        smbopts["domain"] = storage.options["domain"]

    smbver = storage.options.get("samba_version", "3.0")
    if smbver:
        smbopts["vers"] = smbver

    if smbopts:
        opts = " -o '{}'".format(",".join([f"{k}={smbopts[k]}" for k in smbopts]))
    else:
        opts = ""

    cmd = f"mount.cifs {storage.path} {storage.local_path}{opts}"

    if exec_mount(cmd):
        nebula.log.success(f"{storage} mounted successfully")
        storage.mount_attempts = 0
    else:
        if storage.mount_attempts < 5:
            nebula.log.trace(cmd)
            nebula.log.error(f"Unable to mount {storage}")
        storage.last_mount_attempt = time.time()
        storage.mount_attempts += 1


class StorageMonitor(BaseAgent):
    def on_init(self):
        self.status = {}

    def main(self):
        db = nebula.DB()
        db.query("SELECT id, settings FROM storages")
        status: dict[str, Any] = {}

        for id_storage, storage_settings in db.fetchall():
            storage = Storage(
                StorageSettings(
                    id=id_storage,
                    **storage_settings,
                    **status.get(id_storage, {}),
                )
            )

            storage.last_mount_attempt = self.status.get(id_storage, {}).get(
                "last_mount_attempt", 0
            )
            storage.mount_attempts = self.status.get(id_storage, {}).get(
                "mount_attempts", 0
            )

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
