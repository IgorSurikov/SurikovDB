from typing import Generator, Any

from jsonschema import Draft202012Validator

from src.Block import Block
from src.DataBaseCommand import DataBaseCommand
from src.DML.Update import Update
from src.DataBaseStorage import DataBaseStorage
from src.Expression import Expression
from src.JSONQLException import JSONQLException


class Delete(DataBaseCommand):
    json_schema = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "table_name": {"type": "string"},
            "where": Expression.json_schema
        },
        "additionalProperties": False,
        "required": ["type", "table_name"]
    }

    @classmethod
    def from_json(cls, json: Any) -> 'Delete':
        errors = list(Draft202012Validator(cls.json_schema).iter_errors(json))
        if errors:
            raise JSONQLException(errors)

        table_name = json['table_name']
        if 'where' in json:
            filter_exp = Expression(json['where'])
        else:
            filter_exp = Expression(True)
        return cls(table_name, filter_exp)

    def __init__(self, table_name: str, filter_exp: Expression):
        self._table_name = table_name
        self._filter_exp = filter_exp
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        update_map = {'.ROW_IS_DELETED': Expression(True)}

        update = Update(self._table_name, self._filter_exp, update_map)

        for modified_block in update.execute(data_base_storage):
            yield modified_block

        self._result = update.result

    @property
    def result(self):
        return self._result
