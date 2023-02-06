import os
import sys

from nebula.config import config


def get_plugin_path(group=False):
    plugin_path = config.plugin_dir
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
