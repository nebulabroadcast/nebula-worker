Nebula worker
=============

![GitHub release (latest by date)](https://img.shields.io/github/v/release/nebulabroadcast/nebula-worker?style=for-the-badge)
![Maintenance](https://img.shields.io/maintenance/yes/2026?style=for-the-badge)
![Last commit](https://img.shields.io/github/last-commit/nebulabroadcast/nebula-worker?style=for-the-badge)
![Python version](https://img.shields.io/badge/python-3.13-blue?style=for-the-badge)

This repository contains the code for [Nebula 6](https://github.com/nebulabroadcast/nebula) worker node.
The worker node is responsible for processing media files, handling metadata, 
automating transcoding, and controlling playout servers.

This worker node can be run in multiple instances to handle the increasing demand 
for processing and automation of media files. It is a crucial component of the Nebula MAM, 
providing a scalable and efficient solution for processing large amounts of data.

The worker node code is constantly being updated and improved to provide 
the best performance and reliability. If you have any suggestions or encounter any issues, 
feel free to open an issue in the repository or contribute to the code by submitting a pull request.

## Installation

### Docker

Build the image locally from the included `Dockerfile` 
or pull `nebulabroadcast/nebula-worker` from Docker Hub.

Docker compose is usually the best way to run the Nebula worker node,
since all the configuration is done in a single `docker-compose.yml` file.

To mount external storage, the container needs to be privileged.
By running the container in privileged mode, it will have access to the host's resources, 
including the ability to mount external storage. 

The following command will run the container in privileged mode.
Just to let you know, worker node services are started based on the hostnames,
so setting the container hostname is crucial.

In case a worker is running a service that expects incoming network traffic, 
such as play service, don't forget to expose the ports.


```yaml
worker:
  image: nebulabroadcast/nebula-worker:latest
  hostname: worker01
  cap_add:
    - SYS_ADMIN
    - DAC_READ_SEARCH
  privileged: true
  ports:
    - 42100:42100    # play service
    - 6250:6250/udp  # OSC control for casparcg
```

### Bare metal

Nebula is developed and tested on Debian Trixie. Other distributions may work.

Software requirements:

 - Python 3.13
 - FFMpeg
 - cifs-utils (for mounting SMB shares)
 - mediainfo (used by import service)
 - melt (optional)

Installation:

1. Install the required software
2. Clone this repository to `/opt/nebula`
3. Install dependencies with `uv sync`
4. Run the worker with `uv run python -m dispatch`

Nebula worker must run as root to mount external storages.

### Configuration

The worker node can be configured using environment variables.
See the `nebula/config.py` file for a list of available options and their defaults.

The most important directives are:

 - `NEBULA_SITE_NAME` 
 - `NEBULA_POSTGRES` - PostgreSQL connection string (e.g. `postgresql://user:password@host:port/database`)
 - `NEBULA_REDIS` - Redis connection string (e.g. `redis://user:password@host:port/database`)
 - `NEBULA_PLUGIN_DIR` - Path to the directory containing plugins

When installing on bare metal, you may use `.env` file to set the enviroment variables.
