"""Database access: one psycopg connection pool per process (spec 009).

Two APIs share the pool:

- DB(): the handle services and worker plugins have always used. Its
  interface and write semantics don't change: writes need commit(). A read
  borrows a connection and returns it at once; after a write the handle keeps
  its connection (and transaction) until commit() or rollback().
- fetch(), fetchrow(), execute(), executemany(), iterate(), transaction(),
  acquire(): shaped like the backend's nebula.db (nx), synchronous, rows as
  dicts. Outside transaction() every call commits by itself.

Placeholders are %s in both, as with psycopg2.
"""

import threading
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any

import psycopg
from psycopg import ClientCursor, sql
from psycopg.adapt import Dumper
from psycopg.errors import Error, IntegrityError, OperationalError, UndefinedTable
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from nebula.config import config
from nebula.log import log

__all__ = [
    "DB",
    "Error",
    "IntegrityError",
    "OperationalError",
    "acquire",
    "execute",
    "executemany",
    "fetch",
    "fetchrow",
    "iterate",
    "transaction",
]

# A worker process runs only a few threads (the play service the most)
POOL_MAX_SIZE = 8

# Command tags of statements that don't change data. Anything else (INSERT,
# UPDATE, DELETE, DDL, ...) makes a DB() handle keep its connection.
READ_COMMANDS = {"SELECT", "SHOW", "EXPLAIN", "VALUES", "TABLE", "FETCH"}

_pool: ConnectionPool | None = None
_pool_lock = threading.Lock()

# The connection of the current acquire()/transaction() block. Each thread
# starts with an empty context, so threads never share one by accident.
_current: ContextVar[psycopg.Connection | None] = ContextVar(
    "nebula_db_connection", default=None
)
_in_transaction: ContextVar[bool] = ContextVar("nebula_db_transaction", default=False)


def _wait_for_database() -> None:
    """Block until the database accepts connections and has settings.

    The worker may start before the backend has created the schema.
    """
    while True:
        try:
            with psycopg.connect(config.postgres, connect_timeout=3) as conn:
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
                config.postgres,
                min_size=1,
                max_size=POOL_MAX_SIZE,
                timeout=30,
                # A connection that died (e.g. postgres restarted) is replaced
                check=ConnectionPool.check_connection,
                kwargs={"application_name": f"nebula-worker:{log.user}"[:63]},
                open=True,
            )
    return _pool


def _return(conn: psycopg.Connection) -> None:
    """Give a connection back; the pool rolls back or discards it as needed."""
    get_pool().putconn(conn)


#
# Backend-style API
#


@contextmanager
def acquire() -> Iterator[psycopg.Connection]:
    """The current block's connection, or one from the pool for this block.

    A block of its own is committed at the end (rolled back on error).
    """
    conn = _current.get()
    if conn is not None:
        yield conn
        return
    conn = get_pool().getconn()
    token = _current.set(conn)
    try:
        yield conn
        conn.commit()
    except BaseException:
        try:
            conn.rollback()
        except Error:
            pass  # broken connection: the pool discards it
        raise
    finally:
        _current.reset(token)
        _return(conn)


@contextmanager
def transaction() -> Iterator[psycopg.Connection]:
    """Run the block in one transaction. Nested blocks join the outer one.

    DB() handles used inside the block join it too (their commit() and
    rollback() do nothing there).
    """
    with acquire() as conn:
        if _in_transaction.get():
            yield conn
            return
        token = _in_transaction.set(True)
        try:
            with conn.transaction():
                yield conn
        finally:
            _in_transaction.reset(token)


def _params(args: tuple[Any, ...]) -> tuple[Any, ...] | None:
    # None, not (): a query without parameters may contain a literal %
    return args or None


def execute(query: str, *args: Any) -> str:
    """Run a query, return its status (e.g. "UPDATE 3")."""
    with acquire() as conn, conn.cursor() as cur:
        cur.execute(query, _params(args))
        return cur.statusmessage or ""


def executemany(query: str, args: list[tuple[Any, ...]]) -> None:
    with acquire() as conn, conn.cursor() as cur:
        cur.executemany(query, args)


def fetch(query: str, *args: Any) -> list[dict[str, Any]]:
    with acquire() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, _params(args))
        return cur.fetchall()


def fetchrow(query: str, *args: Any) -> dict[str, Any] | None:
    with acquire() as conn, conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, _params(args))
        return cur.fetchone()


def iterate(query: str, *args: Any) -> Iterator[dict[str, Any]]:
    """Yield rows from a server-side cursor, without loading them all.

    Uses its own connection (not the context's) unless inside transaction(),
    so other queries between the rows don't run in the cursor's transaction.
    """
    name = f"nebula_iterate_{uuid.uuid4().hex}"
    if _in_transaction.get() and (conn := _current.get()) is not None:
        with conn.cursor(name=name, row_factory=dict_row) as cur:
            cur.execute(query, _params(args))
            yield from cur
        return
    conn = get_pool().getconn()
    try:
        with conn.cursor(name=name, row_factory=dict_row) as cur:
            cur.execute(query, _params(args))
            yield from cur
        conn.commit()
    finally:
        _return(conn)  # rolls back if the loop was left early


#
# DB() handle
#

_cursor_warning_shown = False


class _TupleDumper(Dumper):
    """Render a tuple as (a, b, ...), as psycopg2 did.

    DB() queries rely on it: `IN %s` with a tuple, multi-row `VALUES %s`.
    Only for client-side binding, which DB() uses.
    """

    def dump(self, obj: tuple[Any, ...]) -> bytes:
        return self.quote(obj)

    def quote(self, obj: tuple[Any, ...]) -> bytes:
        parts = [sql.Literal(item).as_bytes(self.connection) for item in obj]
        return b"(" + b", ".join(parts) + b")"


class DB:
    """Database handle for services and worker plugins.

    Usage is unchanged from the psycopg2 version: query(), fetchall(),
    fetchone(), commit(), rollback(), lastid(), close(). Creating one is cheap:
    it doesn't connect. Safe to use from several threads; each thread reads
    the rows of its own last query.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._local = threading.local()
        # Held from the first write until commit()/rollback()
        self._held: psycopg.Connection | None = None
        # cur/conn were used directly: keep the connection until close()
        self._pinned = False
        self._cursor: ClientCursor | None = None
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
            if _in_transaction.get() and (conn := _current.get()) is not None:
                self._run(conn, query, params)
                return
            conn = get_pool().getconn()
            try:
                write = self._run(conn, query, params)
            except BaseException:
                _return(conn)
                raise
            if write:
                self._held = conn
            else:
                conn.rollback()  # ends the read's implicit transaction
                _return(conn)

    def _run_held(self, query: str, params: Any) -> None:
        assert self._held is not None
        try:
            self._run(self._held, query, params)
        except OperationalError:
            # Connection lost: the transaction is gone, start fresh next time
            conn, self._held, self._cursor = self._held, None, None
            self._pinned = False
            _return(conn)
            raise

    def _run(self, conn: psycopg.Connection, query: str, params: Any) -> bool:
        """Execute and buffer the rows. Returns True for a write."""
        # Client-side binding, like psycopg2: queries behave exactly as before
        with ClientCursor(conn) as cur:
            cur.adapters.register_dumper(tuple, _TupleDumper)
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
            if self._held is None:
                return  # nothing written, or inside transaction() which decides
            conn = self._held
            conn.commit()
            if self._inserted:
                self._lastid = self._read_lastval(conn)
                self._inserted = False
            if not self._pinned:
                self._held = None
                _return(conn)

    def rollback(self) -> None:
        with self._lock:
            if self._held is None:
                return
            conn = self._held
            conn.rollback()
            self._inserted = False
            if not self._pinned:
                self._held = None
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
            if self._held is not None or _in_transaction.get():
                self.query("SELECT LASTVAL()")
                for row in self.fetchall():
                    return row[0]
            elif self._lastid is not None:
                return self._lastid
        raise Exception("Unable to get last id")

    def close(self) -> None:
        with self._lock:
            self._pinned = False
            self._cursor = None
            if self._held is not None:
                conn, self._held = self._held, None
                _return(conn)

    def __del__(self) -> None:
        # Uncommitted writes are rolled back, as when psycopg2 closed the
        # connection of a dropped handle
        held = getattr(self, "_held", None)
        if held is not None:
            try:
                _return(held)
            except Exception:
                pass

    #
    # Direct access, for plugins that used the psycopg2 objects
    #

    def _pin(self) -> psycopg.Connection:
        global _cursor_warning_shown
        if not _cursor_warning_shown:
            log.warning("DB.cur / DB.conn are deprecated, use DB.query()")
            _cursor_warning_shown = True
        with self._lock:
            if self._held is None:
                self._held = get_pool().getconn()
            self._pinned = True
            return self._held

    @property
    def conn(self) -> psycopg.Connection:
        return self._pin()

    @property
    def cur(self) -> ClientCursor:
        conn = self._pin()
        if self._cursor is None:
            self._cursor = ClientCursor(conn)
            self._cursor.adapters.register_dumper(tuple, _TupleDumper)
        return self._cursor

    def __bool__(self) -> bool:
        return True
