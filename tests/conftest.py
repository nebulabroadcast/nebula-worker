"""Worker tests, in two tiers like the backend:

    tests/unit         no database writes
    tests/integration  write to the database and clean up after themselves

Both need the stack's database: importing nebula connects to it (and waits
forever when it can't), so this checks first and stops with a clear message.
On the host: NEBULA_POSTGRES=postgres://nebula:nebula@localhost:54320/nebula
(`make test-worker` in the workbench sets it).
"""

import os

import psycopg2
import pytest

url = os.environ.get("NEBULA_POSTGRES", "postgres://nebula:nebula@postgres/nebula")
try:
    psycopg2.connect(url, connect_timeout=3).close()
except psycopg2.OperationalError as e:
    pytest.exit(f"Database not available at {url}: {e}", returncode=3)
