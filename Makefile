VERSION=$(shell sed -n 's/__version__ = \"\(.*\)\"/\1/p' nebula/version.py)

check:
	uv version $(VERSION)
	uv run ruff check . --select=I --fix
	uv run ruff format .
	uv run ruff check . --fix --unsafe-fixes
	uv run mypy .

build:
	docker build -t nebulabroadcast/nebula-worker:dev .

dist: check build
	docker push nebulabroadcast/nebula-worker:dev

setup-hooks:
	@echo "Setting up Git hooks..."
	@mkdir -p .git/hooks
	@echo '#!/bin/sh\n\n# Navigate to the repository root directory\ncd "$$(git rev-parse --show-toplevel)"\n\n# Execute the linting command from the Makefile\nmake check\n\n# Check the return code of the make command\nif [ $$? -ne 0 ]; then\n  echo "Linting failed. Commit aborted."\n  exit 1\nfi\n\n# If everything is fine, allow the commit\nexit 0' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Git hooks set up successfully."
