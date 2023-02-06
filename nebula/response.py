import json
from http import HTTPStatus


class NebulaResponse:
    def __init__(self, response=200, message=None, **kwargs):
        self.dict = {"response": response, "message": message}
        self.dict.update(kwargs)

    @property
    def json(self):
        return json.dumps(self.dict)

    @property
    def response(self):
        return self["response"]

    @property
    def message(self):
        return self["message"] or HTTPStatus(self.response).name

    @property
    def data(self):
        return self.get("data", {})

    @property
    def is_success(self):
        return self.response < 400

    @property
    def is_error(self):
        return self.response >= 400

    def get(self, key, default=False):
        return self.dict.get(key, default)

    def __getitem__(self, key):
        return self.dict[key]

    def __len__(self):
        return self.is_success
