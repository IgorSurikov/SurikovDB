import os.path
import struct
from typing import Generator

from src.Table import Table
from src.constants import *
from src.Block import Block
from struct import calcsize, unpack_from


class DataBaseStorage():
    TABLE_DESCRIPTOR_BLOCK_COUNT = 16

    def __init__(self, path: str):
        self.file = open(path, 'r+b', buffering=16 * BLOCK_SIZE)
        file_size = os.path.getsize(path)

        if file_size % BLOCK_SIZE:
            raise Exception('Incorrect DB file')
        else:
            self.block_count = file_size // BLOCK_SIZE

        if self.block_count == 0:
            for _ in range(self.TABLE_DESCRIPTOR_BLOCK_COUNT):
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

    def write_table(self, table: Table) -> None:
        last_iteration = None
        for i in self.table_iter():
            last_iteration = i

        if last_iteration is None:
            block_index = 0
            ptr = 0
        else:
            _, block_index, ptr = last_iteration

        ddl = table.encode_ddl
        ddl = struct.pack(f'{TABLE_IS_DELETED_F}{TABLE_DDL_SIZE_F}', False, len(ddl)) + ddl

        if len(ddl) + ptr > BLOCK_SIZE:
            block_index += 1
            ptr = 0
            if block_index >= self.TABLE_DESCRIPTOR_BLOCK_COUNT:
                raise Exception('No space to create table')

        b = self.read_block(block_index)
        b.override(ptr, ddl)
        self.write_block(b)

    def table_iter(self) -> Generator[tuple[Table, int, int], None, None]:
        for block_index in range(self.TABLE_DESCRIPTOR_BLOCK_COUNT):
            b = self.read_block(block_index)
            ptr = 0
            while True:
                table_is_deleted, ddl_size = unpack_from(f'{TABLE_IS_DELETED_F}{TABLE_DDL_SIZE_F}', b, ptr)
                ptr += calcsize(f'{TABLE_IS_DELETED_F}{TABLE_DDL_SIZE_F}')
                if ddl_size == 0:
                    break

                if not table_is_deleted:
                    yield Table.from_encode_ddl(b[ptr: ptr + ddl_size]), block_index, ptr + ddl_size

                ptr += ddl_size
