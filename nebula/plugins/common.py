import os
import sys

import nebula


def get_plugin_path(group: str | None = None) -> str:
    plugin_path = os.path.join(
        nebula.storages[nebula.settings.system.worker_plugin_storage].local_path,
        nebula.settings.system.worker_plugin_path,
    )
    if group:
        plugin_path = os.path.join(plugin_path, group)
    if not os.path.isdir(plugin_path):
        return ""
    return plugin_path


def load_common_scripts():
    if get_plugin_path():
        common_dir = get_plugin_path("common")
        if (
            os.path.isdir(common_dir)
            and os.listdir(common_dir)
            and common_dir not in sys.path
        ):
            sys.path.insert(0, common_dir)
