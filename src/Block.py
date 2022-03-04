from struct import unpack_from, pack, calcsize
from typing import NoReturn

from src.TableMetaData import TableMetaData
from src.constants import *


class Block(bytearray):

    def __init__(self, data: bytes, index: int = 0):
        assert len(data) == BLOCK_SIZE, 'Incorrect block size: {}'.format(len(data))
        super().__init__(data)

        self.idx = index

    @classmethod
    def empty(cls, index: int = 0) -> 'Block':
        return cls(b'\0' * BLOCK_SIZE, index)

    @property
    def is_empty(self) -> bool:
        return self == b'\0' * BLOCK_SIZE

    def override(self, offset: int, data: bytes) -> None:
        assert offset + len(data) <= BLOCK_SIZE

        self[offset:offset + len(data)] = data

    def free(self):
        self.override(0, b'\0' * BLOCK_SIZE)


class TableMetaDataBlock(Block):

    def __init__(self, data: bytes, index: int = 0):
        super().__init__(data, index)

        self._ddl_size = unpack_from(f'{TABLE_DDL_SIZE_F}', self, 0)[0]
        self._ddl_start_ptr = 0 + calcsize(f'{TABLE_DDL_SIZE_F}')

        self._pointer_count_start_ptr = self._ddl_start_ptr + self._ddl_size
        self._pointer_count = unpack_from(f'{POINTER_COUNT_F}', self, self._pointer_count_start_ptr)[0]

        self._pointer_list_start_ptr = self._pointer_count_start_ptr + calcsize(f'{POINTER_COUNT_F}')
        self._pointer_list_end_ptr = self._pointer_list_start_ptr + (self._pointer_count * calcsize(f'{POINTER_F}'))

    @classmethod
    def from_block(cls, block: Block) -> 'TableMetaDataBlock':
        return cls(bytes(block), block.idx)

    def create_table(self, table_meta_data: TableMetaData) -> NoReturn:
        ddl = table_meta_data.encode_ddl
        ddl = pack(f'{TABLE_DDL_SIZE_F}', len(ddl)) + ddl

        if len(ddl) > BLOCK_SIZE:
            raise Exception('DDL size is more than BLOCK_SIZE')

        self.override(0, ddl)

    def drop_table(self) -> NoReturn:
        self.free()

    def get_table_meta_data(self) -> TableMetaData:
        if self.is_empty:
            raise Exception('No Table Metadata')

        return TableMetaData.from_encode_ddl(self[self._ddl_start_ptr: self._ddl_start_ptr + self._ddl_size])

    def add_pointers(self, pointer_list: list[int]):

        pointer_list_encode = pack(f'{POINTER_F}' * len(pointer_list), *pointer_list)

        if self._pointer_list_end_ptr + len(pointer_list_encode) > BLOCK_SIZE:
            raise Exception('No space for pointer list')

        self.override(self._pointer_list_end_ptr, pointer_list_encode)
        self._pointer_list_end_ptr += calcsize(f'{POINTER_F}' * len(pointer_list))
        self._pointer_count += len(pointer_list)

    def get_pointers(self) -> list[int]:
        return list(
            unpack_from(
                f'{POINTER_F}' * self._pointer_count,
                self,
                self._pointer_list_start_ptr))


class DataBlock(Block):

    def __init__(self, data: bytes, index: int, row_format: str):
        super().__init__(data, index)

        self._row_format = row_format
        self._row_count = unpack_from(f'{ROW_COUNT_F}', self, 0)[0]

        self._row_list_start_ptr = 0 + calcsize(f'{ROW_COUNT_F}')
        self._row_list_end_ptr = self._row_list_start_ptr + (self._row_count * calcsize(f'{row_format}'))

    @classmethod
    def from_block(cls, block: Block, row_format: str) -> 'DataBlock':
        return cls(bytes(block), block.idx, row_format)

    def add_rows(self, row_list: list[tuple]) -> NoReturn:

        row_list_flat = sum(row_list, ())
        row_list_encode = pack(f'{self._row_format}' * len(row_list), *row_list_flat)

        if self._row_list_end_ptr + len(row_list_encode) > BLOCK_SIZE:
            raise Exception('No space for row list')

        self.override(self._row_list_end_ptr, row_list_encode)
        self._row_list_end_ptr += calcsize(f'{self._row_format}' * len(row_list))
        self._row_count += len(row_list)

    def get_rows(self) -> list[tuple]:
        return list(
            unpack_from(
                f'{self._row_format}' * self._row_count,
                self,
                self._row_list_start_ptr))
