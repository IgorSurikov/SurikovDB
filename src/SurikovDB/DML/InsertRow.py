from typing import Generator

from SurikovDB.Block import TableMetaDataBlock, DataBlock, Block
from SurikovDB.DataBaseCommand import DataBaseCommand
from SurikovDB.DataBaseStorage import DataBaseStorage
from SurikovDB.constants import ROW_TYPE, POINTER_F


class InsertRow(DataBaseCommand):
    def __init__(self, table_name: str, row: ROW_TYPE):
        self._table_name = table_name
        self._row = row
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        table_meta_data, block_index = data_base_storage.get_table_meta_data(self._table_name)

        if len(self._row) != len(table_meta_data.column_list):
            raise Exception(f'Invalid row size - row :{self._row}, column list - {table_meta_data.column_list}')

        for v, c in zip(self._row, table_meta_data.column_list):
            if not c.is_valid_value(v):
                raise Exception(f'Column {c.name} - value {v} is not valid')

        row = (False,) + self._row
        row_format = table_meta_data.row_struct_format

        data_block_table_meta_data = TableMetaDataBlock.from_block(data_base_storage.read_block(block_index))

        data_block_pointer_lvl1 = DataBlock.from_block(
            data_base_storage.read_block(data_block_table_meta_data.data_block_pointer_lvl1), POINTER_F)

        data_block_pointer_lvl2 = DataBlock.from_block(
            data_base_storage.read_block(data_block_pointer_lvl1.read_last_row()[0]),
            POINTER_F)

        data_block_table_data = DataBlock.from_block(
            data_base_storage.read_block(data_block_pointer_lvl2.read_last_row()[0]),
            row_format)

        if data_block_table_data.is_full:
            if data_block_pointer_lvl2.is_full:
                data_block_pointer_lvl2 = DataBlock.from_block(data_base_storage.allocate_block(), POINTER_F)
                yield data_block_pointer_lvl1
                data_block_pointer_lvl1.write_row((data_block_pointer_lvl2.idx,))
                data_base_storage.write_block(data_block_pointer_lvl1)

            data_block_table_data = DataBlock.from_block(data_base_storage.allocate_block(), row_format)
            yield data_block_pointer_lvl2
            data_block_pointer_lvl2.write_row((data_block_table_data.idx,))
            data_base_storage.write_block(data_block_pointer_lvl2)

        yield data_block_table_data
        data_block_table_data.write_row(row)
        data_base_storage.write_block(data_block_table_data)

    @property
    def result(self):
        return self._result
