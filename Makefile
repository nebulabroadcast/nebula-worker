VERSION=$(shell poetry run python -c 'import nebula' --version)

check: check_version
	poetry run isort .
	poetry run black .
	poetry run flake8 .
	poetry run mypy .

check_version:
	echo $(VERSION)
	sed -i "s/version = \".*\"/version = \"$(VERSION)\"/" pyproject.toml

build:
	docker build -t nebulabroadcast/nebula-worker:latest .
