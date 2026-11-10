"""Worker tests, in two tiers like the backend:

    tests/unit         no database (`import nebula` doesn't connect)
    tests/integration  the stack database; see tests/integration/conftest.py

`make test-worker` / `make test-worker-int` in the workbench.
"""
