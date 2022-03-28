import os.path
import struct
import typing
from collections import namedtuple
from typing import Generator, NoReturn

from src.BlockStorage import BlockStorage
from src.Expression import Expression
from src.RowSet import RowSet
from src.TableMetaData import TableMetaData
from src.constants import *
from src.Block import Block, TableMetaDataBlock, DataBlock
from struct import calcsize, unpack_from


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

        table_meta_data_block = TableMetaDataBlock.from_block(self.read_block(table_meta_data_block_index))
        pointer_on_block_of_pointers_list = table_meta_data_block.get_pointers()

        for i in pointer_on_block_of_pointers_list:
            pointer_on_block_of_table_data_list = DataBlock.from_block(self.read_block(i), POINTER_F).read_rows()
            for j in pointer_on_block_of_table_data_list:
                table_data_block = DataBlock.from_block(self.read_block(j[0]), row_format)
                yield table_data_block

    def drop_table(self, table_name):
        for table_meta_data, block_index in self.table_meta_data_gen():
            if table_meta_data.name == table_name:
                b = self.read_block(block_index)
                b.free()
                self.write_block(b)
                break
        else:
            raise Exception('Table is not exist')

    def create_table(self, table_meta_data: TableMetaData) -> NoReturn:

        for i in range(self.TABLE_META_DATA_BLOCK_COUNT):
            b = self.read_block(i)
            if b.is_empty:
                b = TableMetaDataBlock.from_block(b)
                break
        else:
            raise Exception('No space for table')

        b.create_table(table_meta_data)
        data_block_for_pointers = DataBlock.from_block(self.allocate_block(), POINTER_F)
        data_block_for_table_data = self.allocate_block()

        data_block_for_pointers.write_row(
            (data_block_for_table_data.idx,))

        b.add_pointers([data_block_for_pointers.idx])

        self.write_block(data_block_for_pointers)
        self.write_block(data_block_for_table_data)
        self.write_block(b)

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

        return RowSet([c.name for c in table_meta_data.column_list], row_gen())

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
