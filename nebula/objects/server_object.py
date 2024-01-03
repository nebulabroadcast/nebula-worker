import json

from nxtools import slugify

from nebula.db import DB
from nebula.log import log
from nebula.messaging import messaging

from .base import BaseObject


def create_ft_index(meta):
    ft = {}
    if "subclips" in meta:
        weight = 8
        for sc in [k.get("title", "") for k in meta["subclips"]]:
            try:
                for word in slugify(sc, make_set=True, min_length=3):
                    if word not in ft:
                        ft[word] = weight
                    else:
                        ft[word] = max(ft[word], weight)
            except Exception:
                log.error("Unable to slugify subclips data")
    for key in meta:
        # TODO
        # if key not in meta_types:
        #     continue
        # weight = meta_types[key].get("fulltext")
        weight = 0
        if type(meta[key]) == str:
            weight = 1

        if not weight:
            continue
        try:
            for word in slugify(meta[key], make_set=True, min_length=3):
                if word not in ft:
                    ft[word] = weight
                else:
                    ft[word] = max(ft[word], weight)
        except Exception:
            log.error(f"Unable to slugify key {key} with value {meta[key]}")
    return ft


class ServerObject(BaseObject):
    def __init__(self, id: int | None = None, **kwargs):
        if "db" in kwargs:
            self._db = kwargs["db"]
        super(ServerObject, self).__init__(id, **kwargs)

    @property
    def db(self):
        if not hasattr(self, "_db"):
            log.debug(f"{self} is opening DB connection")
            self._db = DB()
        return self._db

    def load(self, id):
        self.db.query(f"SELECT meta FROM {self.table_name} WHERE id = {id}")
        try:
            self.meta = self.db.fetchall()[0][0]
        except IndexError:
            log.error(
                f"Unable to load {self.__class__.__name__}"
                f"ID:{id}. Object does not exist"
            )
            return False

    def save(self, **kwargs):
        super(ServerObject, self).save(**kwargs)
        is_new = self.is_new
        if is_new:
            self._insert(**kwargs)
        else:
            self._update(**kwargs)
            self.invalidate()
        if self.text_changed or is_new:
            self.update_ft_index(is_new)
        if kwargs.get("commit", True):
            self.db.commit()
        self.text_changed = self.meta_changed = False
        self.is_new = False
        if kwargs.get("notify", True):
            messaging.send(
                "objects_changed", objects=[self.id], object_type=self.object_type
            )

    def _insert(self, **kwargs):
        cols = []
        vals = []
        if self.id:
            cols.append("id")
            vals.append(self.id)
        for col in self.db_cols:
            cols.append(col)
            vals.append(self[col])
        if self.id:
            cols.append("meta")
            vals.append(json.dumps(self.meta))

        if cols:
            query = "INSERT INTO {} ({}) VALUES ({}) RETURNING id".format(
                self.table_name, ", ".join(cols), ", ".join(["%s"] * len(cols))
            )
        else:
            query = f"""
                INSERT INTO {self.table_name}
                DEFAULT VALUES RETURNING id
            """
        self.db.query(query, vals)

        if not self.id:
            new_id = self.db.fetchall()[0][0]
            assert new_id, "Unable to insert new object, database returned no ID"
            self["id"] = new_id
            self.db.query(
                f"UPDATE {self.table_name} SET meta=%s WHERE id=%s",
                [json.dumps(self.meta), new_id],
            )

    def _update(self, **kwargs):
        assert self.id > 0
        cols = ["meta"]
        vals = [json.dumps(self.meta)]

        for col in self.db_cols:
            cols.append(col)
            vals.append(self[col])

        query = "UPDATE {} SET {} WHERE id=%s".format(
            self.table_name, ", ".join([key + "=%s" for key in cols])
        )
        self.db.query(query, vals + [self.id])

    def update_ft_index(self, is_new=False):
        if not is_new:
            self.db.query(
                "DELETE FROM ft WHERE object_type=%s AND id=%s",
                [self.object_type_id, self.id],
            )
        ft = create_ft_index(self.meta)
        if not ft:
            return
        args = [(self.id, self.object_type_id, ft[word], word) for word in ft]
        tpls = ",".join(["%s"] * len(args))
        self.db.query(
            f"""
            INSERT INTO ft (id, object_type, weight, value)
            VALUES {tpls}""",
            args,
        )

    def invalidate(self):
        """Invalidate all cache objects which references this one"""
        pass

    def delete_children(self):
        pass

    def delete(self):
        if not self.id:
            return
        log.info(f"Deleting {self}")
        self.delete_children()
        self.db.query("DELETE FROM {} WHERE id=%s".format(self.table_name), [self.id])
        self.db.query(
            "DELETE FROM ft WHERE object_type=%s AND id=%s",
            [self.object_type_id, self.id],
        )
        self.db.commit()
