from nebula.objects.base import BaseObject, object_helper


class User(BaseObject):
    object_type_id = 4
    table_name = "users"
    db_cols = ["login", "password"]
    required = ["login", "password"]

    def __getitem__(self, key):
        if key == "title":
            return self.meta.get("login", "Anonymous")
        return super().__getitem__(key)

    def has_right(self, key: str, val: bool | list[int] = True, anyval: bool = False):
        if self["is_admin"]:
            return True
        key = f"can/{key}"
        if not self[key]:
            return False
        if anyval:
            return True
        return self[key] is True or (isinstance(self[key], list) and val in self[key])


object_helper["user"] = User
