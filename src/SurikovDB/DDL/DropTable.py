from typing import Generator, Any

from jsonschema import Draft202012Validator

from SurikovDB.Block import Block
from SurikovDB.DataBaseCommand import DataBaseCommand
from SurikovDB.DataBaseStorage import DataBaseStorage
from SurikovDB.JSONQLException import JSONQLException


class DropTable(DataBaseCommand):
    json_schema = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "table_name": {"type": "string"},
        },
        "additionalProperties": False,
        "required": ["type", "table_name"]
    }

    @classmethod
    def from_json(cls, json: Any) -> 'DropTable':
        errors = list(Draft202012Validator(cls.json_schema).iter_errors(json))
        if errors:
            raise JSONQLException(errors)

        table_name = json['table_name']
        return cls(table_name)

    def __init__(self, table_name: str):
        self._table_name = table_name
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        for table_meta_data, block_index in data_base_storage.table_meta_data_gen():
            if table_meta_data.name == self._table_name:
                b = data_base_storage.read_block(block_index)
                yield b
                b.free()
                data_base_storage.write_block(b)
                break
        else:
            raise Exception('Table is not exist')

    @property
    def result(self):
        return self._result
