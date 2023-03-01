check:
	poetry run isort .
	poetry run black .
	poetry run flake8 .
	poetry run mypy .

build:
	docker build -t nebulabroadcast/nebula-worker:latest .
