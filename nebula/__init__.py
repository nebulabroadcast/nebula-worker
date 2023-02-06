from nebula.settings import load_settings, settings
from nebula.log import log
from nebula.config import config
from nebula.db import DB
from nebula.messaging import messaging
from nebula.storages import storages

#
# Keep linters happy
#

assert DB
assert config
assert settings
assert storages

#
# Setup stuff
#

log.user = "nebula"
load_settings()

#
# Helpers
#


def msg(topic, **kwargs):
    messaging.send(topic, **kwargs)
