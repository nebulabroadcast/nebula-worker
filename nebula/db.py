import time
import psycopg2

from urllib.parse import urlparse

from nebula.config import config
from nebula.log import log

NEBULA_IS_INSTALLED: bool = False


class DB:
    def __init__(self):
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
                    self.query("SELECT * FROM settings")
                    assert self.fetchone()
                except Exception:
                    log.traceback()
                    log.warning("Waiting for DB schema")
                    time.sleep(3)
                else:
                    NEBULA_IS_INSTALLED = True
                    break

    def lastid(self):
        self.query("SELECT LASTVAL()")
        return self.fetchall()[0][0]

    def query(self, query, *args):
        self.cur.execute(query, *args)

    def fetchone(self):
        return self.cur.fetchone()

    def fetchall(self):
        return self.cur.fetchall()

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

    def close(self):
        self.conn.close()

    def __len__(self):
        return True
