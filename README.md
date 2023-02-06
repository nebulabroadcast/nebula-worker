Nebula worker
=============

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

Build the image locally from the included `Dockerfile` or pull `nebulabroadcast/nebula-worker` from Docker Hub.

In order to mount external storage, the container need to be privileged.
By running the container in privileged mode, it will have access to the host's resources, 
including the ability to mount external storage. 

### Bare metal

Nebula is developed and tested on Debian Buster. Other distributions may work.

Software requirements:

 - Python 3.10+
 - Poetry
 - FFMpeg
 - cifs-utils (for mounting SMB shares)

1. Install the required software
2. Clone this repository to `/opt/nebula`
3. Install dependencies with `poetry install`
4. Run the worker with `poetry run python -m dispatch`

Nebula worker must run as root in order to mount external storages.

### Configuration

The worker node can be configured using environment variables.
See the `nebula/config.py` file for a list of available options.

Most important directives are:

 - NEBULA_SITE_NAME 
 - NEBULA_POSTGRES - PostgreSQL connection string (e.g. `postgresql://user:password@host:port/database`)
 - NEBULA_REDIS - Redis connection string (e.g. `redis://user:password@host:port/database`)
 - NEBULA_PLUGIN_DIR - Path to the directory containing plugins
