import json
import queue
import socket
import threading
import time

import redis

from nebula.config import config
from nebula.log import log

HOSTNAME = socket.gethostname()


class Messaging:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.queue = queue.Queue()
        self.lock = threading.Lock()

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

    def __call__(self, method, **data):
        self.queue.put([method, data])
        self.lock.acquire()
        while not self.queue.empty():
            qm, qd = self.queue.get()
            self.send_message(qm, **qd)
        self.lock.release()

    def send(self, method, **data):
        if not (self.connection and self.channel):
            if not self.connect():
                time.sleep(0.1)
                return

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
