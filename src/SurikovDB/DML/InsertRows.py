from typing import Generator, Any

from jsonschema import Draft202012Validator

from SurikovDB.Block import Block
from SurikovDB.DML.InsertRow import InsertRow
from SurikovDB.DataBaseCommand import DataBaseCommand
from SurikovDB.DataBaseStorage import DataBaseStorage
from SurikovDB.JSONQLException import JSONQLException
from SurikovDB.constants import ROW_TYPE


class InsertRows(DataBaseCommand):
    json_schema = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "table_name": {"type": "string"},
            "rows": {
                "type": "array",
                "items": {
                    "type": "array",
                    "items": {
                        "anyOf": [
                            {"type": "string"},
                            {"type": "number"},
                        ]}
                },
                "minItems": 1,
            }

        },
        "additionalProperties": False,
        "required": ["type", "table_name", "rows"]
    }

    @classmethod
    def from_json(cls, json: Any) -> 'InsertRows':
        errors = list(Draft202012Validator(cls.json_schema).iter_errors(json))
        if errors:
            raise JSONQLException(errors)

        table_name = json['table_name']
        row_list = [tuple(i) for i in json['rows']]
        return cls(table_name, row_list)

    def __init__(self, table_name: str, row_list: list[ROW_TYPE]):
        self._table_name = table_name
        self._row_list = row_list
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        for row in self._row_list:
            insert_row = InsertRow(self._table_name, row)
            for modified_block in insert_row.execute(data_base_storage):
                yield modified_block

        self._result = len(self._row_list)

    @property
    def result(self):
        return self._result
