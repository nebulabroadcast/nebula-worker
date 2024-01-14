import imp
import os
import sys
import time
from typing import TYPE_CHECKING, Optional

import nebula
from nebula.base_service import BaseService
from nebula.plugins import get_plugin_path

if TYPE_CHECKING:
    from nebula.plugins.worker import WorkerPlugin


class Service(BaseService):
    def on_init(self):
        self.exec_init: str | None = None
        self.exec_main: str | None = None
        self.plugin: Optional["WorkerPlugin"] = None

        if "script" in self.settings.attrib:
            fname = self.settings.attrib["script"]
            result = self.load_from_script(fname)
        else:
            result = self.load_from_settings()

        if not result:
            nebula.log.error("Unable to load worker. Shutting down")
            self.shutdown(no_restart=True)

    def load_from_script(self, fname: str) -> bool:
        if not fname.lower().endswith(".py"):
            fname += ".py"
        workerdir = get_plugin_path("worker")
        if not workerdir:
            nebula.log.error("Plugin path is not set. Storage unmouted?")
            time.sleep(5)
            sys.exit(0)
        script_path = os.path.join(workerdir, fname)
        mod_name, _ = os.path.splitext(fname)

        if not os.path.exists(script_path):
            nebula.log.error(f"Plugin {fname} not found")
            return False

        py_mod = imp.load_source(mod_name, script_path)

        if "Plugin" not in dir(py_mod):
            nebula.log.error(f"No plugin class found in {fname}")
            return False

        nebula.log.debug(f"Loading plugin {mod_name}")
        self.plugin = py_mod.Plugin(self)
        if not self.plugin:
            nebula.log.error(f"Unable to load plugin {mod_name}")
            return False
        self.plugin.on_init()
        return True

    def load_from_settings(self):
        exec_init = self.settings.find("init")
        if exec_init and exec_init.text:
            self.exec_init = exec_init.text
            exec(self.exec_init)

        exec_main = self.settings.find("main")
        if exec_main and exec_main.text:
            self.exec_main = exec_main.text
        return True

    def on_main(self):
        if self.plugin:
            self.plugin.on_main()
        elif self.exec_main:
            exec(self.exec_main)
