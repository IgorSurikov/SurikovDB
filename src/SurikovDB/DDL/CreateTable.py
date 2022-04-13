from typing import Generator, Any

from jsonschema import Draft202012Validator

from SurikovDB.DDL.Column import ColumnLong, ColumnChar, ColumnFactory
from SurikovDB.JSONQLException import JSONQLException
from SurikovDB.DDL.TableMetaData import TableMetaData
from SurikovDB.Block import Block, TableMetaDataBlock, DataBlock
from SurikovDB.DataBaseCommand import DataBaseCommand
from SurikovDB.DataBaseStorage import DataBaseStorage
from SurikovDB.constants import POINTER_F


class CreateTable(DataBaseCommand):
    json_schema = {
        "type": "object",
        "properties": {
            "type": {"type": "string"},
            "table_name": {"type": "string"},
            "columns": {
                "type": "array",
                "items":
                    {"anyOf": [
                        ColumnLong.json_schema,
                        ColumnChar.json_schema,
                    ]},
                "minItems": 1,

            }
        },
        "additionalProperties": False,
        "required": ["type", "table_name", "columns"]

    }

    @classmethod
    def from_json(cls, json: Any) -> 'CreateTable':
        errors = list(Draft202012Validator(cls.json_schema).iter_errors(json))
        if errors:
            raise JSONQLException(errors)

        table_name = json['table_name']
        column_list = [ColumnFactory.column_from_json(c) for c in json['columns']]
        table_meta_data = TableMetaData(table_name, column_list)
        return cls(table_meta_data)

    def __init__(self, table_meta_data: TableMetaData):
        self._table_meta_data = table_meta_data
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        for t in data_base_storage.table_meta_data_gen():
            if self._table_meta_data.name == t.table_meta_data.name:
                raise Exception('Table name must be unique')


        for i in range(data_base_storage.TABLE_META_DATA_BLOCK_COUNT):
            b = data_base_storage.read_block(i)
            if b.is_empty:
                b = TableMetaDataBlock.from_block(b)
                break
        else:
            raise Exception('No space for table')

        data_block_pointer_lvl1 = DataBlock.from_block(data_base_storage.allocate_block(), POINTER_F)
        data_block_pointer_lvl2 = DataBlock.from_block(data_base_storage.allocate_block(), POINTER_F)
        data_block_table_data = data_base_storage.allocate_block()

        yield data_block_pointer_lvl2
        data_block_pointer_lvl2.write_row((data_block_table_data.idx,))

        yield data_block_pointer_lvl1
        data_block_pointer_lvl1.write_row((data_block_pointer_lvl2.idx,))

        yield b
        b.create_table(self._table_meta_data, data_block_pointer_lvl1.idx)

        data_base_storage.write_block(data_block_pointer_lvl1)
        data_base_storage.write_block(data_block_pointer_lvl2)
        data_base_storage.write_block(data_block_table_data)
        data_base_storage.write_block(b)

    @property
    def result(self):
        return self._result
