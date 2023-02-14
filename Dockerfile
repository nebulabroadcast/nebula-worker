FROM nebulabroadcast/nebula-base:latest

ENV PYTHONBUFFERED=1

RUN mkdir -p /opt/nebula
WORKDIR /opt/nebula
COPY ./pyproject.toml /opt/nebula/pyproject.toml
RUN poetry install --no-interaction --no-ansi
COPY . /opt/nebula

CMD ["python", "-m", "dispatch"]
