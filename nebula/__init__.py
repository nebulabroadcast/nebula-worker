__all__ = [
    "Asset",
    "Item",
    "Bin",
    "Event",
    "User",
    "DB",
    "db",
    "config",
    "log",
    "LogLevel",
    "messaging",
    "msg",
    "settings",
    "storages",
]
import sys

from nebula.version import __version__

if "--version" in sys.argv:
    print(__version__)
    sys.exit(0)

from nebula import db
from nebula.config import config
from nebula.db import DB
from nebula.log import LogLevel, log
from nebula.messaging import messaging
from nebula.objects import Asset, Bin, Event, Item, User
from nebula.settings import settings
from nebula.storages import storages

#
# Setup stuff
#

log.user = "nebula"
log.level = LogLevel[config.log_level.upper()]

#
# Helpers
#


def msg(topic, **kwargs):
    messaging.send(topic, **kwargs)
