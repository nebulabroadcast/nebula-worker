import imp
import os

import nebula
from nebula.plugins import get_plugin_path


class PlayoutPlugins:
    def __init__(self, service):
        self.service = service
        self.plugins = []

    def load(self):
        self.plugins = []
        bpath = get_plugin_path("playout")
        if not bpath:
            nebula.log.warning(f"Playout plugins directory {bpath} does not exist")
            return

        for plugin_name in self.service.channel.plugins:
            plugin_file = plugin_name + ".py"
            plugin_path = os.path.join(bpath, plugin_file)

            if not os.path.exists(plugin_path):
                nebula.log.error(f"Plugin {plugin_name} does not exist")
                continue

            try:
                py_mod = imp.load_source(plugin_name, plugin_path)
            except Exception:
                nebula.log.traceback(f"Unable to load plugin {plugin_name}")
                continue

            if "Plugin" not in dir(py_mod):
                nebula.log.error(f"No plugin class found in {plugin_file}")
                continue

            nebula.log.info(f"Initializing plugin {plugin_name}")
            self.plugins.append(py_mod.Plugin(self.service))
            self.plugins[-1].title = self.plugins[-1].title or plugin_name.capitalize()
        nebula.log.info("All plugins initialized")

    def __getitem__(self, key):
        return self.plugins[key]
