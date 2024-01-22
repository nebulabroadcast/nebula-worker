VERSION=$(shell poetry run python -c 'import nebula' --version)

check_version:
	echo $(VERSION)
	sed -i "s/version = \".*\"/version = \"$(VERSION)\"/" pyproject.toml

check: check_version
	poetry run black . && \
	poetry run ruff --fix . && \
	poetry run mypy .


build:
	docker build -t nebulabroadcast/nebula-worker:latest .

dist: build
	docker push nebulabroadcast/nebula-worker:latest

