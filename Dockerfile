FROM python:3.11-bullseye

ENV PYTHONBUFFERED=1

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get -y install \
    cifs-utils \
    zlib1g-dev \
    build-essential \
    mediainfo \
    curl

# Build deps (ffmpeg)

RUN curl https://raw.githubusercontent.com/immstudios/installers/master/install.ffmpeg.sh | bash \
  && rm -rf /tmp/install.ffmpeg.sh

# Nebula codebase

RUN mkdir -p /opt/nebula
COPY . /opt/nebula
WORKDIR /opt/nebula

# Python deps

RUN pip install -U pip && pip install poetry
RUN poetry config virtualenvs.create false \
  && poetry install --no-interaction --no-ansi

CMD ["python", "-m", "dispatch"]
