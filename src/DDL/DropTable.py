from typing import Generator

from src.Block import Block
from src.DataBaseCommand import DataBaseCommand
from src.DataBaseStorage import DataBaseStorage


class DropTable(DataBaseCommand):
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
