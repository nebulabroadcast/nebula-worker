import os
import signal
import sys
import time
from typing import TYPE_CHECKING, Any, Type

import nebula
from dispatch.service_monitor import ServiceMonitor
from dispatch.storage_monitor import StorageMonitor

if TYPE_CHECKING:
    from dispatch.agents import BaseAgent

# from dispatch.system_monitor import SystemMonitor

orig_dir = os.getcwd()
if orig_dir != "/opt/nebula":
    os.chdir("/opt/nebula")

nebula.log.user = "dispatch"


class NebulaDispatch:
    agent_list: dict[str, Type["BaseAgent"]] = {
        "storage-monitor": StorageMonitor,
        "service-monitor": ServiceMonitor,
        #        "system-monitor": SystemMonitor,
    }

    def __init__(self) -> None:
        self.should_run = True
        self.agents: list["BaseAgent"] = []
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum: Any, frame: Any) -> None:
        _ = signum, frame
        self.should_run = False

    def __call__(self) -> None:
        for AgentClass in self.agent_list.values():
            try:
                agent = AgentClass()
                self.agents.append(agent)
            except Exception:
                nebula.log.traceback()
                self.shutdown()
                nebula.log.error(f"Unable to start {AgentClass.__name__}")

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

    def shutdown(self) -> None:
        for agent in self.agents:
            agent.shutdown()
        while self.is_running:
            time.sleep(0.5)
        return None

    @property
    def is_running(self) -> bool:
        return any(agent.is_running for agent in self.agents)


if __name__ == "__main__":
    dispatch = NebulaDispatch()
    dispatch()
    nebula.log.info("Nebula exited gracefully")
