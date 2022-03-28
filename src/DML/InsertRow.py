from typing import Generator

from src.Block import TableMetaDataBlock, DataBlock, Block
from src.DML.DMLCommand import DMLCommand
from src.DataBaseStorage import DataBaseStorage
from src.constants import *


class InsertRow(DMLCommand):
    def __init__(self, table_name: str, row: ROW_TYPE):
        self._table_name = table_name
        self._row = row
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        table_meta_data, block_index = data_base_storage.get_table_meta_data(self._table_name)

        for v, c in zip(self._row, table_meta_data.column_list):
            if not c.is_valid_value(v):
                raise Exception(f'Column {c.name} - value {v} is not valid')

        row = (False,) + self._row
        row_format = table_meta_data.row_struct_format

        table_meta_data_block = TableMetaDataBlock.from_block(data_base_storage.read_block(block_index))

        data_block_for_pointers = DataBlock.from_block(
            data_base_storage.read_block(table_meta_data_block.get_last_pointer()),
            POINTER_F)

        data_block_for_table_data = DataBlock.from_block(
            data_base_storage.read_block(data_block_for_pointers.read_last_row()[0]),
            row_format)

        if data_block_for_table_data.is_full:
            if data_block_for_pointers.is_full:
                data_block_for_pointers = DataBlock.from_block(data_base_storage.allocate_block(), POINTER_F)
                yield table_meta_data_block
                table_meta_data_block.add_pointers([data_block_for_pointers.idx])
                data_base_storage.write_block(table_meta_data_block)

            data_block_for_table_data = DataBlock.from_block(data_base_storage.allocate_block(), row_format)
            yield data_block_for_pointers
            data_block_for_pointers.write_row((data_block_for_table_data.idx,))
            data_base_storage.write_block(data_block_for_pointers)

        yield data_block_for_table_data
        data_block_for_table_data.write_row(row)
        data_base_storage.write_block(data_block_for_table_data)

    @property
    def result(self):
        return self._result
