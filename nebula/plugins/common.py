import importlib
import importlib.util
import os
import sys
from types import ModuleType

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


def import_module(name: str, path: str) -> ModuleType:
    if (spec := importlib.util.spec_from_file_location(name, path)) is None:
        raise ModuleNotFoundError(f"Module {name} not found")
    if (module := importlib.util.module_from_spec(spec)) is None:
        raise ImportError(f"Module {name} cannot be imported")
    if spec.loader is None:
        raise ImportError(f"Module {name} cannot be imported. No loader found.")
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module
