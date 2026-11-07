FROM nebulabroadcast/nebula-worker-base:2026.07
COPY --from=ghcr.io/astral-sh/uv:0.9.8 /uv /usr/local/bin/uv

ENV PYTHONUNBUFFERED=1

RUN mkdir -p /opt/nebula
WORKDIR /opt/nebula

COPY ./pyproject.toml ./uv.lock /opt/nebula/
# Exactly the versions in uv.lock (pip alone would resolve pyproject's ranges)
RUN uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && pip install --break-system-packages -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
COPY . /opt/nebula

CMD ["python", "-m", "dispatch"]
