FROM nebulabroadcast/nebula-worker-base:6.1.0

ENV PYTHONUNBUFFERED=1

RUN mkdir -p /opt/nebula
WORKDIR /opt/nebula

COPY ./pyproject.toml ./uv.lock /opt/nebula/
RUN pip install --break-system-packages -e .
COPY . /opt/nebula

CMD ["python", "-m", "dispatch"]
