import time
from typing import Any
from urllib.parse import urlparse

import psycopg2

from nebula.config import config
from nebula.log import log

NEBULA_IS_INSTALLED: bool = False


class DB:
    def __init__(self) -> None:
        result = urlparse(config.postgres)
        global NEBULA_IS_INSTALLED

        conn_dict = {
            "user": result.username,
            "password": result.password,
            "host": result.hostname,
            "port": result.port,
            "database": result.path[1:],
        }

        while True:
            try:
                self.conn = psycopg2.connect(**conn_dict)
            except psycopg2.OperationalError:
                log.warning("Unable to connect to database, retrying in 1 second...")
                time.sleep(1)
                continue
            break

        self.cur = self.conn.cursor()

        if not NEBULA_IS_INSTALLED:
            while True:
                try:
                    self.query("SELECT * FROM settings LIMIT 1")
                except psycopg2.errors.UndefinedTable:
                    self.rollback()
                    log.warning("Database not installed, retrying in 1 second...")
                    time.sleep(1)
                    continue

                try:
                    assert self.fetchall()
                except Exception:
                    log.warning("Waiting for settings, retrying in 1 second...")
                    time.sleep(3)
                else:
                    NEBULA_IS_INSTALLED = True
                    break

    def lastid(self) -> int:
        self.query("SELECT LASTVAL()")
        for row in self.fetchall():
            return row[0]
        raise Exception("Unable to get last id")

    def query(self, query: str, *args: Any) -> None:
        self.cur.execute(query, *args)

    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()

    def commit(self) -> None:
        self.conn.commit()

    def rollback(self) -> None:
        self.conn.rollback()

    def close(self) -> None:
        self.conn.close()

    def __bool__(self) -> bool:
        return True
