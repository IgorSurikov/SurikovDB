from typing import Generator

from src.constants import *
from src.TableMetaData import TableMetaData
from src.Block import Block, TableMetaDataBlock, DataBlock
from src.DataBaseCommand import DataBaseCommand
from src.DataBaseStorage import DataBaseStorage


class CreateTable(DataBaseCommand):
    def __init__(self, table_meta_data: TableMetaData):
        self._table_meta_data = table_meta_data
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
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
