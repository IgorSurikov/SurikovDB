from typing import Generator, Any

from jsonschema import Draft202012Validator

from SurikovDB.Block import Block
from SurikovDB.DataBaseCommand import DataBaseCommand
from SurikovDB.DataBaseStorage import DataBaseStorage
from SurikovDB.Expression import Expression
from SurikovDB.JSONQLException import JSONQLException


class Select(DataBaseCommand):
    json_schema = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "select": {
                "type": "array",
                "items": Expression.json_schema},
            "from": {"type": "string"},
            "join": {
                "type": "object",
                "additionalProperties": Expression.json_schema
            },
            "where":
                Expression.json_schema

        },
        "additionalProperties": False,
        "required": ["type", "select", "from"]

    }

    @classmethod
    def from_json(cls, json: Any) -> 'Select':
        errors = list(Draft202012Validator(cls.json_schema).iter_errors(json))
        if errors:
            raise JSONQLException(errors)
        return cls(json)

    def __init__(self, json: any):
        self._json = json
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        json = self._json
        table_name = json['from']
        row_set_result = data_base_storage.scan(table_name)

        if 'join' in json:
            for t, exp in json['join'].items():
                row_set = data_base_storage.scan(t)
                row_set_result = row_set_result.join(row_set, Expression(exp))

        if 'where' in json:
            row_set_result = row_set_result.where(Expression(json['where']))

        if json['select'] != ['*']:
            row_set_result = row_set_result.select([Expression(e) for e in json['select']])

        self._result = row_set_result
        yield from []  # execute() need to be generator

    @property
    def result(self):
        return self._result
