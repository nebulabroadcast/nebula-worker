from inspect import cleandoc
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import fromstring as parse_xml

from nebula.db import DB
from nebula.log import log
from nebula.objects import Asset

from .exec_context import DEFAULT_EXEC_CONTEXT


def proc_cond(cond: str | None) -> str | None:
    if not cond:
        return None
    cond = cleandoc(cond.strip())
    if not cond:
        return None
    return cond


class Action:
    start_if: str | None = None
    create_if: str | None = None
    skip_if: str | None = None

    def __init__(self, id_action: int, title: str, settings: Element):
        self.id = id_action
        self.title = title
        self.settings = settings
        try:
            create_if = settings.findall("create_if")[0]
        except IndexError:
            self.create_if = None
        else:
            self.create_if = proc_cond(create_if.text)

        try:
            start_if = settings.findall("start_if")[0]
        except IndexError:
            self.start_if = None
        else:
            self.start_if = proc_cond(start_if.text)

        try:
            skip_if = settings.findall("skip_if")[0]
        except IndexError:
            self.skip_if = None
        else:
            self.skip_if = proc_cond(skip_if.text)

    @property
    def created_key(self) -> str:
        return f"job_created/{self.id}"

    def should_create(self, asset: Asset) -> bool:
        if not self.create_if:
            return False
        safe_globals = {"asset": asset, **DEFAULT_EXEC_CONTEXT}
        try:
            return eval(self.create_if, {"__builtins__": None}, safe_globals)
        except Exception as e:
            log.error(f"Error evaluating create_if for action {self.id}: {e}")
            return False

    def should_start(self, asset: Asset) -> bool:
        if not self.start_if:
            return False
        safe_globals = {"asset": asset, **DEFAULT_EXEC_CONTEXT}
        try:
            return eval(self.start_if, {"__builtins__": None}, safe_globals)
        except Exception as e:
            log.error(f"Error evaluating start_if for action {self.id}: {e}")
            return False

    def should_skip(self, asset: Asset) -> bool:
        if not self.skip_if:
            return False
        safe_globals = {"asset": asset, **DEFAULT_EXEC_CONTEXT}
        try:
            return eval(self.skip_if, {"__builtins__": None}, safe_globals)
        except Exception as e:
            log.error(f"Error evaluating skip_if for action {self.id}: {e}")
            return False


class Actions:
    data: dict[int, Action]

    def __init__(self) -> None:
        self.data = {}

    def load(self, id_action: int) -> None:
        db = DB()
        db.query("SELECT title, settings FROM actions WHERE id = %s", [id_action])
        for title, settings in db.fetchall():
            self.data[id_action] = Action(id_action, title, parse_xml(settings))

    def __getitem__(self, key: int) -> Action:
        if key not in self.data:
            self.load(key)
        return self.data[key]


actions = Actions()
