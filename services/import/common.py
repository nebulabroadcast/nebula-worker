from pathlib import Path

from pydantic import BaseModel

import nebula


def create_error(import_file: Path, message: str) -> None:
    """Write the error message to a .txt file beside the import file."""
    message_path = import_file.with_suffix(".txt")
    if message_path.exists() and message_path.read_text() == message:
        return
    nebula.log.error(message)
    message_path.write_text(message)


class ImportDefinition(BaseModel):
    action_id: int
    import_dir: Path
    identifier: str = "id"
    profile: str
