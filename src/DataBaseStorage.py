import os.path
import struct
import typing
from collections import namedtuple
from typing import Generator, NoReturn

from src.TableMetaData import TableMetaData
from src.constants import *
from src.Block import Block, TableMetaDataBlock
from struct import calcsize, unpack_from


class DataBaseStorage():
    TABLE_META_DATA_BLOCK_COUNT = 10
    table_meta_data_gen_result = typing.NamedTuple('table_meta_data_gen_result',
                                                   [('table_meta_data', TableMetaData), ('block_index', int)])

    def __init__(self, path: str):
        self.file = open(path, 'r+b', buffering=16 * BLOCK_SIZE)
        file_size = os.path.getsize(path)

        if file_size % BLOCK_SIZE:
            raise Exception('Incorrect DB file')
        else:
            self.block_count = file_size // BLOCK_SIZE

        if self.block_count == 0:
            for _ in range(self.TABLE_META_DATA_BLOCK_COUNT):
                self.allocate_block()

    def read_block(self, index: int) -> Block:
        if index > self.block_count:
            raise Exception('Block is not exist')

        self.file.seek(index * BLOCK_SIZE)
        block = Block(self.file.read(BLOCK_SIZE), index)
        return block

    def write_block(self, block: Block) -> None:
        if block.idx > self.block_count:
            raise Exception('Block is not exist')

        self.file.seek(block.idx * BLOCK_SIZE)
        self.file.write(block)

    def allocate_block(self) -> Block:
        block = Block.empty(self.block_count)
        self.block_count += 1
        self.write_block(block)
        return block

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
        self.write_block(b)

    def table_meta_data_gen(self) -> Generator[table_meta_data_gen_result, None, None]:
        for block_index in range(self.TABLE_META_DATA_BLOCK_COUNT):
            b = TableMetaDataBlock.from_block(self.read_block(block_index))

            if b.is_empty:
                continue

            yield self.table_meta_data_gen_result(b.get_table_meta_data(), block_index)
