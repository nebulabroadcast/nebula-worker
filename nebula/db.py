"""Database access: one psycopg connection pool per process.

DB() is the handle services and worker plugins have always used. Its
interface and write semantics don't change: writes need commit(). A read
borrows a connection and returns it at once; after a write the handle keeps
its connection (and transaction) until commit() or rollback().

Placeholders are %s, as with psycopg2.
"""

import threading
import time
from typing import Any
from urllib.parse import urlparse

import psycopg
from psycopg import ClientCursor, sql
from psycopg.adapt import Dumper
from psycopg.errors import Error, IntegrityError, OperationalError, UndefinedTable
from psycopg_pool import ConnectionPool

from nebula.config import config
from nebula.log import log

__all__ = ["DB", "Error", "IntegrityError", "OperationalError"]

# A worker process runs only a few threads (the play service the most)
POOL_MAX_SIZE = 8

# Command tags of statements that don't change data. Anything else (INSERT,
# UPDATE, DELETE, DDL, ...) makes a DB() handle keep its connection.
READ_COMMANDS = {"SELECT", "SHOW", "EXPLAIN", "VALUES", "TABLE", "FETCH"}

_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()


def connection_kwargs() -> dict[str, Any]:
    """Connection parameters from NEBULA_POSTGRES.

    Parsed like the psycopg2 version did (urlparse, no percent-decoding), so
    a password with %, @ or other special characters keeps working.
    """
    url = urlparse(config.postgres)
    kwargs = {
        "user": url.username,
        "password": url.password,
        "host": url.hostname,
        "port": url.port,
        "dbname": url.path[1:],
    }
    return {key: value for key, value in kwargs.items() if value is not None}


def _wait_for_database() -> None:
    """Block until the database accepts connections and has settings.

    The worker may start before the backend has created the schema.
    """
    while True:
        try:
            with psycopg.connect(**connection_kwargs(), connect_timeout=3) as conn:
                try:
                    if conn.execute("SELECT 1 FROM settings LIMIT 1").fetchone():
                        return
                    log.warning("Waiting for settings, retrying in 1 second...")
                except UndefinedTable:
                    log.warning("Database not installed, retrying in 1 second...")
        except OperationalError:
            log.warning("Unable to connect to database, retrying in 1 second...")
        time.sleep(1)


def get_pool() -> ConnectionPool:
    global _pool
    with _pool_lock:
        if _pool is None:
            _wait_for_database()
            _pool = ConnectionPool(
                "",
                min_size=1,
                max_size=POOL_MAX_SIZE,
                timeout=30,
                # A connection that died (e.g. postgres restarted) is replaced
                check=ConnectionPool.check_connection,
                kwargs={
                    **connection_kwargs(),
                    "application_name": f"nebula-worker:{log.user}"[:63],
                },
                open=True,
            )
    return _pool


def _return(conn: psycopg.Connection) -> None:
    """Give a connection back; the pool rolls back or discards it as needed."""
    get_pool().putconn(conn)


class _TupleDumper(Dumper):
    """Render a tuple as (a, b, ...), as psycopg2 did.

    DB() queries rely on it: `IN %s` with a tuple, multi-row `VALUES %s`,
    nested tuples for `(a, b) IN %s`. Only for client-side binding.
    """

    def dump(self, obj: tuple[Any, ...]) -> bytes:
        return self.quote(obj)

    def quote(self, obj: tuple[Any, ...]) -> bytes:
        parts = [
            self.quote(item)
            if isinstance(item, tuple)
            else sql.Literal(item).as_bytes(self.connection)
            for item in obj
        ]
        return b"(" + b", ".join(parts) + b")"


def _compat_cursor(conn: psycopg.Connection) -> ClientCursor:
    """A cursor that binds parameters like psycopg2: client side, tuples."""
    cur = ClientCursor(conn)
    cur.adapters.register_dumper(tuple, _TupleDumper)
    return cur


def _is_select(query: str) -> bool:
    return query.lstrip()[:6].upper() == "SELECT"


class DB:
    """Database handle for services and worker plugins.

    Usage is unchanged from the psycopg2 version: query(), fetchall(),
    fetchone(), commit(), rollback(), lastid(), close(). Creating one is
    cheap: it doesn't connect. Safe to use from several threads; each thread
    reads the rows of its own last query.

    lastid() after commit() returns LASTVAL() read at the commit; if that
    transaction's INSERT used no sequence, the value can be stale (as it
    could be with psycopg2 within one session).
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._local = threading.local()
        # Held from the first write until commit()/rollback()
        self._held: psycopg.Connection | None = None
        self._inserted = False
        self._lastid: int | None = None

    #
    # Queries
    #

    def query(self, query: str, *args: Any) -> None:
        params = args[0] if args else None
        with self._lock:
            if self._held is not None:
                self._run_held(query, params)
                return
            conn = get_pool().getconn()
            try:
                # A plain SELECT needs no transaction: autocommit saves the
                # BEGIN and the COMMIT. It may still write (SELECT f() of a
                # writing function); that is kept, as commit() kept it before.
                conn.autocommit = _is_select(query)
                write = self._run(conn, query, params)
                if write and not conn.autocommit:
                    self._held = conn
                    return
                if not conn.autocommit:
                    conn.commit()  # SHOW, VALUES, ...: end the transaction
            finally:
                if self._held is not conn:
                    try:
                        conn.autocommit = False
                    except Error:
                        pass  # broken connection: the pool discards it
                    _return(conn)

    def _run_held(self, query: str, params: Any) -> None:
        assert self._held is not None
        try:
            self._run(self._held, query, params)
        except OperationalError:
            # Connection lost: the transaction is gone, start fresh next time
            self._drop_held()
            raise

    def _run(self, conn: psycopg.Connection, query: str, params: Any) -> bool:
        """Execute and buffer the rows. Returns True for a write."""
        with _compat_cursor(conn) as cur:
            cur.execute(query, params)
            rows = cur.fetchall() if cur.description else []
            command = (cur.statusmessage or "").split(" ", 1)[0].upper()
        self._local.rows = rows
        self._local.pos = 0
        if command == "INSERT":
            self._inserted = True
        return command not in READ_COMMANDS

    def fetchall(self) -> list[tuple[Any, ...]]:
        rows = getattr(self._local, "rows", [])
        pos = getattr(self._local, "pos", 0)
        self._local.pos = len(rows)
        return rows[pos:]

    def fetchone(self) -> tuple[Any, ...] | None:
        rows = getattr(self._local, "rows", [])
        pos = getattr(self._local, "pos", 0)
        if pos >= len(rows):
            return None
        self._local.pos = pos + 1
        return rows[pos]

    #
    # Transactions
    #

    def commit(self) -> None:
        with self._lock:
            if self._held is not None:
                self._end(commit=True)

    def rollback(self) -> None:
        with self._lock:
            if self._held is not None:
                self._end(commit=False)

    def _end(self, *, commit: bool) -> None:
        """End the held transaction and give the connection back."""
        conn = self._held
        assert conn is not None
        try:
            if commit:
                conn.commit()
                if self._inserted:
                    self._lastid = self._read_lastval(conn)
            else:
                conn.rollback()
        except BaseException:
            self._drop_held()  # broken: the pool discards it
            raise
        finally:
            self._inserted = False
        self._held = None
        _return(conn)

    def _drop_held(self) -> None:
        conn, self._held = self._held, None
        self._inserted = False
        if conn is not None:
            _return(conn)

    @staticmethod
    def _read_lastval(conn: psycopg.Connection) -> int | None:
        # Session value: still readable on this connection after the commit
        try:
            row = conn.execute("SELECT LASTVAL()").fetchone()
        except Error:
            row = None  # the insert used no sequence
        conn.rollback()
        return row[0] if row else None

    def lastid(self) -> int:
        with self._lock:
            if self._held is not None:
                self.query("SELECT LASTVAL()")
                for row in self.fetchall():
                    return row[0]
            elif self._lastid is not None:
                return self._lastid
        raise Exception("Unable to get last id")

    def close(self) -> None:
        self.rollback()

    def __del__(self) -> None:
        # Uncommitted writes are rolled back, as when psycopg2 closed the
        # connection of a dropped handle
        held = getattr(self, "_held", None)
        if held is None:
            return
        try:
            _return(held)
        except Exception:
            pass

    def __bool__(self) -> bool:
        return True
