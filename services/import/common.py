import os

from nxtools import FileObject, get_temp
from pydantic import BaseModel, Field

import nebula


def create_error(import_file: FileObject, message: str) -> None:
    """Create a sidecar file with the error message for the import file."""
    dir_name = import_file.dir_name
    message_path = os.path.join(dir_name, f"{import_file.base_name}.txt")

    if os.path.exists(message_path):
        original_message = open(message_path).read()
        if original_message == message:
            return None

    nebula.log.error(message)
    with open(message_path, "w") as f:
        f.write(message)


def temp_file(id_storage, ext):
    temp_dir = os.path.join(nebula.storages[id_storage].local_path, ".nx", "creating")
    if not os.path.isdir(temp_dir):
        try:
            os.makedirs(temp_dir)
        except Exception:
            nebula.log.traceback()
            return False
    return get_temp(ext, temp_dir)


class ImportDefinition(BaseModel):
    action_id: int
    import_dir: str = Field(...)
    backup_dir: str | None = Field(None)
    identifier: str = Field("id")
    profile: str
