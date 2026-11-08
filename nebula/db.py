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
from urllib.parse import urlparse

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


#
# Backend-style API
#


@contextmanager
def acquire() -> Iterator[psycopg.Connection]:
    """The current block's connection, or one from the pool for this block.

    Outside transaction() the connection is in autocommit: every statement
    commits by itself, as with asyncpg in the backend.
    """
    conn = _current.get()
    if conn is not None:
        yield conn
        return
    conn = get_pool().getconn()
    token = _current.set(conn)
    try:
        conn.autocommit = True
        yield conn
    finally:
        _current.reset(token)
        try:
            conn.autocommit = False  # DB() handles expect transactions
        except Error:
            pass  # broken connection: the pool discards it
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
    finally:
        try:
            conn.rollback()  # read only; also when the loop was left early
        except Error:
            pass
        _return(conn)


#
# DB() handle
#

_cursor_warning_shown = False


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
    fetchone(), commit(), rollback(), lastid(), close(), cur, conn. Creating
    one is cheap: it doesn't connect. Safe to use from several threads; each
    thread reads the rows of its own last query.

    Read query() results with fetchall()/fetchone(); `cur` is a separate
    cursor for code that executes on it directly.

    lastid() after commit() returns LASTVAL() read at the commit; if that
    transaction's INSERT used no sequence, the value can be stale (as it
    could be with psycopg2 within one session).
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._local = threading.local()
        # Held from the first write until commit()/rollback()
        self._held: psycopg.Connection | None = None
        # cur/conn were used: _held is a connection of its own, kept until
        # close() or until nothing references it, as with psycopg2
        self._pinned = False
        self._own_conn = False  # _held came from psycopg.connect, not the pool
        self._cursor: ClientCursor | None = None
        # A SET ran on the held connection: reset it before others use it
        self._session_dirty = False
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
            write = False
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
        elif command == "SET" and not self._pinned:
            self._session_dirty = True
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
        """End the held transaction; give the connection back unless pinned."""
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
        if not self._pinned:
            self._held = None
            self._reset_session(conn)
            _return(conn)

    def _drop_held(self) -> None:
        conn, self._held = self._held, None
        self._inserted = self._session_dirty = False
        if conn is None:
            return
        pinned, self._pinned = self._pinned, False
        self._cursor = None
        if self._own_conn:
            self._own_conn = False
            conn.close()
            return
        if pinned:
            # A cursor handed out may still be referenced: close the
            # connection so nobody else gets it while that cursor exists
            conn.close()
        _return(conn)

    def _reset_session(self, conn: psycopg.Connection) -> None:
        # SET outlives the transaction; the next borrower mustn't inherit it
        if not self._session_dirty:
            return
        self._session_dirty = False
        try:
            conn.execute("RESET ALL")
            conn.commit()
        except Error:
            pass  # broken: the pool discards it

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
            if self._held is None:
                return
            if self._pinned:
                self._drop_held()  # closes it, rolling back, as psycopg2 did
            else:
                self._end(commit=False)

    def __del__(self) -> None:
        # Uncommitted writes are rolled back, as when psycopg2 closed the
        # connection of a dropped handle. A pinned connection is left to its
        # cursor's references and closes when they go.
        held = getattr(self, "_held", None)
        if held is None or getattr(self, "_own_conn", False):
            return
        try:
            if getattr(self, "_pinned", False):
                held.close()  # a retained cursor must not share it (see above)
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
            if not self._pinned:
                if self._held is None:
                    # A connection of its own: it lives as long as the handle
                    # or a cursor handed out references it, like psycopg2's
                    conn = psycopg.connect(
                        **connection_kwargs(), cursor_factory=ClientCursor
                    )
                    conn.adapters.register_dumper(tuple, _TupleDumper)
                    self._held = conn
                    self._own_conn = True
                # else: keep the pooled connection with the uncommitted write,
                # so cur/conn see it, as psycopg2's shared connection did
                self._pinned = True
            assert self._held is not None
            return self._held

    @property
    def conn(self) -> psycopg.Connection:
        return self._pin()

    @property
    def cur(self) -> ClientCursor:
        conn = self._pin()
        if self._cursor is None:
            self._cursor = ClientCursor(conn)
        return self._cursor

    def __bool__(self) -> bool:
        return True
