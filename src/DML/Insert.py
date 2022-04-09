from typing import Generator, Any

from jsonschema import Draft202012Validator

from src.Block import TableMetaDataBlock, DataBlock, Block
from src.DML.InsertRow import InsertRow
from src.DML.Select import Select
from src.DataBaseCommand import DataBaseCommand
from src.DataBaseStorage import DataBaseStorage
from src.JSONQLException import JSONQLException
from src.RowSet import RowSet
from src.constants import *


class Insert(DataBaseCommand):
    json_schema = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "table_name": {"type": "string"},
            "select": Select.json_schema
        },
        "additionalProperties": False,
        "required": ["type", "table_name", "select"]
    }

    @classmethod
    def from_json(cls, json: Any) -> 'Insert':
        errors = list(Draft202012Validator(cls.json_schema).iter_errors(json))
        if errors:
            raise JSONQLException(errors)

        table_name = json['table_name']
        select = Select.from_json(json['select'])
        return cls(table_name, select)

    def __init__(self, table_name: str, select: Select):
        self._table_name = table_name
        self._select = select
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        select = self._select
        for modified_block in select.execute(data_base_storage):
            yield modified_block

        row_set: RowSet = select.result

        row_count = 0
        for row in row_set.row_gen:
            insert_row = InsertRow(self._table_name, row)
            for modified_block in insert_row.execute(data_base_storage):
                yield modified_block
            row_count += 1

        self._result = row_count

    @property
    def result(self):
        return self._result
