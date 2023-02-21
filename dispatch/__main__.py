import os
import signal
import sys
import time

import nebula
from dispatch.service_monitor import ServiceMonitor
from dispatch.storage_monitor import StorageMonitor
from dispatch.system_monitor import SystemMonitor

orig_dir = os.getcwd()
if orig_dir != "/opt/nebula":
    os.chdir("/opt/nebula")

nebula.log.user = "dispatch"


class NebulaDispatch:
    agent_list = {
        "storage-monitor": StorageMonitor,
        "service-monitor": ServiceMonitor,
        #        "system-monitor": SystemMonitor,
    }

    def __init__(self):
        self.should_run = True
        self.agents = []
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum, frame):
        self.should_run = False

    def __call__(self):
        for agent_name, Agent in self.agent_list.items():
            # env_name = f"NEBULA_DISABLE_{agent_name.upper().replace('-', '_')}"
            # if os.environ.get(env_name) or f"--disable-{agent_name}" in sys.argv:
            #     os.environ[env_name] = "1"
            #     nebula.log.info(f"Agent {agent_name} is disabled")
            #     continue
            try:
                self.agents.append(Agent())
            except Exception:
                nebula.log.traceback()
                self.shutdown()
                nebula.log.critical(f"Unable to start {Agent.__name__}")

        while self.should_run:
            try:
                time.sleep(5)
            except KeyboardInterrupt:
                break
            except Exception:
                nebula.log.traceback()

        nebula.log.info("Nebula is shutting down")

        try:
            self.shutdown()
        except KeyboardInterrupt:
            print()
            nebula.log.warning("Immediate shutdown enforced. This may cause problems")
            sys.exit(1)

    def shutdown(self):
        for agent in self.agents:
            agent.shutdown()
        while self.is_running:
            time.sleep(0.5)

    @property
    def is_running(self):
        return any([agent.is_running for agent in self.agents])


if __name__ == "__main__":
    dispatch = NebulaDispatch()
    dispatch()
    nebula.log.info("Nebula exited gracefully")
