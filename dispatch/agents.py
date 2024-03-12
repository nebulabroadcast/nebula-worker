import threading
import time

import nebula


class BaseAgent:
    def __init__(self, once=False) -> None:
        self.first_run = True
        self.thread = None
        self.is_running = False
        self.should_run = True
        try:
            self.on_init()
        except Exception:
            nebula.log.traceback()
            nebula.log.critical(f"Unable to start {self.__class__.__name__}")
        if once:
            self.main()
        else:
            self.thread = threading.Thread(target=self.run, daemon=True)
            self.thread.start()

    def on_init(self) -> None:
        pass

    def on_shutdown(self) -> None:
        pass

    def shutdown(self) -> None:
        self.should_run = False

    def run(self) -> None:
        self.is_running = True
        nebula.log.info(f"Starting {self.__class__.__name__}")
        while self.should_run:
            try:
                self.main()
            except Exception:
                nebula.log.traceback()
            self.first_run = False
            time.sleep(2)
        self.on_shutdown()
        self.is_running = False

    def main(self) -> None:
        pass
