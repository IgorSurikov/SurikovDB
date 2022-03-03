from struct import unpack_from, pack, calcsize
from typing import Union, NoReturn

from src.TableMetaData import TableMetaData
from src.constants import BLOCK_SIZE, BYTEORDER, POINTER_SIZE, POINTERS_PER_BLOCK, TABLE_IS_DELETED_F, TABLE_DDL_SIZE_F


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

    @classmethod
    def from_block(cls, block: Block):
        return cls(bytes(block), block.idx)

    def create_table(self, table_meta_data: TableMetaData) -> NoReturn:
        ddl = table_meta_data.encode_ddl
        ddl = pack(f'{TABLE_DDL_SIZE_F}', len(ddl)) + ddl

        if len(ddl) > BLOCK_SIZE:
            raise Exception('DDL size is more than BLOCK_SIZE')

        self.override(0, ddl)

    def drop_table(self):
        self.free()

    def get_table_meta_data(self) -> TableMetaData:
        if self.is_empty:
            raise Exception('No TableMetadata')

        ptr = 0
        ddl_size = unpack_from(f'{TABLE_DDL_SIZE_F}', self, ptr)[0]
        ptr += calcsize(f'{TABLE_DDL_SIZE_F}')
        return TableMetaData.from_encode_ddl(self[ptr: ptr + ddl_size])


