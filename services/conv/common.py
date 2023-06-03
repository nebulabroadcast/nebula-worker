import os

import nebula

from nxtools import get_temp
from nebula.storages import storages

def temp_file(id_storage, ext):
    temp_dir = os.path.join(storages[id_storage].local_path, ".nx", "creating")
    if not os.path.isdir(temp_dir):
        try:
            os.makedirs(temp_dir)
        except Exception:
            nebula.log.traceback()
            return False
    return get_temp(ext, temp_dir)


class ConversionError(Exception):
    pass
