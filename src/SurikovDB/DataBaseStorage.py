import typing
from typing import Generator

from SurikovDB.BlockStorage import BlockStorage
from SurikovDB.RowSet import RowSet
from SurikovDB.DDL.TableMetaData import TableMetaData
from SurikovDB.Block import TableMetaDataBlock, DataBlock
from SurikovDB.constants import *


class DataBaseStorage(BlockStorage):
    TABLE_META_DATA_BLOCK_COUNT = 10
    table_meta_data_gen_result = typing.NamedTuple('table_meta_data_gen_result',
                                                   [('table_meta_data', TableMetaData), ('block_index', int)])

    def __init__(self, path: str):
        super().__init__(path, BLOCK_SIZE)

        if self.block_count == 0:
            for _ in range(self.TABLE_META_DATA_BLOCK_COUNT):
                self.allocate_block()

    def scan_table_data_block_gen(self,
                                  table_meta_data: TableMetaData,
                                  table_meta_data_block_index: int) -> Generator[DataBlock, None, None]:

        row_format = table_meta_data.row_struct_format
        data_block_table_meta_data = TableMetaDataBlock.from_block(self.read_block(table_meta_data_block_index))

        data_block_pointer_lvl1 = DataBlock.from_block(
            self.read_block(data_block_table_meta_data.data_block_pointer_lvl1), POINTER_F)

        for i in data_block_pointer_lvl1.read_rows():
            data_block_pointer_lvl2_list = DataBlock.from_block(self.read_block(i[0]), POINTER_F).read_rows()
            for j in data_block_pointer_lvl2_list:
                data_block_table_data = DataBlock.from_block(self.read_block(j[0]), row_format)
                yield data_block_table_data

    def scan(self, table_name: str) -> RowSet:
        for table_meta_data, block_index in self.table_meta_data_gen():
            if table_meta_data.name == table_name:
                break
        else:
            raise Exception('Table is not exist')

        def row_gen() -> Generator[ROW_TYPE, None, None]:
            for table_data_block in self.scan_table_data_block_gen(table_meta_data, block_index):
                row_list = table_data_block.read_rows()
                for k in row_list:
                    if not k[0]:
                        yield k[1:]

        return RowSet([c.name for c in table_meta_data.column_list], row_gen, [f'{table_name}'])

    def table_meta_data_gen(self) -> Generator[table_meta_data_gen_result, None, None]:
        for block_index in range(self.TABLE_META_DATA_BLOCK_COUNT):
            b = TableMetaDataBlock.from_block(self.read_block(block_index))

            if b.is_empty:
                continue

            yield self.table_meta_data_gen_result(b.get_table_meta_data(), block_index)

    def get_table_meta_data(self, table_name: str) -> table_meta_data_gen_result:
        for i in self.table_meta_data_gen():
            if i.table_meta_data.name == table_name:
                return i
        else:
            raise Exception('Table is not exist')
