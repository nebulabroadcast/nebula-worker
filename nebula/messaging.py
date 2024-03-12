import json
import queue
import socket
import threading
import time
from typing import Any

import redis

from nebula.config import config
from nebula.log import log

HOSTNAME = socket.gethostname()


class Messaging:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue: queue.Queue[Any] = queue.Queue()

        self.main_loop = threading.Thread(target=self.send_thread)
        self.main_loop.daemon = True
        self.main_loop.start()

    def connect(self):
        self.channel = f"nebula-{config.site_name}"
        log.debug(f"Connecting messaging to {config.redis}", handlers=None)
        try:
            self.connection = redis.from_url(
                config.redis,
                decode_responses=True,
                socket_timeout=3,
                socket_connect_timeout=3,
            )
        except Exception:
            log.traceback("Unable to connect redis", handlers=None)
            return False
        return True

    def send_thread(self):
        while True:
            if self.queue.empty():
                time.sleep(0.01)
                continue
            qm, qd = self.queue.get()
            self.send(qm, **qd)

    def __call__(self, method, **data):
        self.queue.put([method, data])

    def send(self, method, **data):
        if not (self.connection and self.channel):
            if not self.connect():
                time.sleep(0.1)
                return

        assert self.connection and self.channel

        message = json.dumps(
            [
                time.time(),
                config.site_name,
                HOSTNAME,
                method,
                data,
            ]
        )
        try:
            self.connection.publish(self.channel, message)
        except redis.exceptions.ConnectionError:
            log.error("Unable to connect Redis to send a message.", handlers=None)
            time.sleep(1)
            self.connect()
        except Exception:
            log.traceback(handlers=None)
            self.connect()


messaging = Messaging()
log.messaging = messaging
