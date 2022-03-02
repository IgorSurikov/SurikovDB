import struct
from struct import calcsize, unpack_from
from src.Column import Column, ColumnFactory
from src.constants import *


class Table:
    def __init__(self, name: str, column_list: list[Column]):
        if len(name.encode()) > calcsize(TABLE_NAME_F):
            raise Exception(f"Table name '{name}' more then table name max size({calcsize(TABLE_NAME_F)})")

        self._name = name
        self._column_list = column_list
        self._column_count = len(column_list)
        self._check_row_size()  # throw exception
        self._check_ddl_size()  # throw exception

    @property
    def row_struct_format(self) -> str:
        return ''.join([c.struct_format for c in self._column_list])

    @property
    def encode_ddl(self) -> bytes:

        ddl = [
            self._name.encode(),
            len(self._column_list)
        ]

        for c in self._column_list:
            ddl.extend([c.name.encode(), c.code])

        return struct.pack(self._get_ddl_struct_format(), *ddl)

    @classmethod
    def from_encode_ddl(cls, encode_ddl: bytes) -> 'Table':
        ptr = 0

        table_name = unpack_from(TABLE_NAME_F, encode_ddl)[0].strip(b'\x00').decode()
        ptr = calcsize(TABLE_NAME_F)

        column_count = unpack_from(COLUMN_COUNT_F, encode_ddl, ptr)[0]
        ptr += calcsize(COLUMN_COUNT_F)

        column_list = []
        for i in range(column_count):
            col_name = unpack_from(COLUMN_NAME_F, encode_ddl, ptr)[0].strip(b'\x00').decode()
            ptr += calcsize(COLUMN_NAME_F)
            col_code = unpack_from(COLUMN_CODE_F, encode_ddl, ptr)[0]
            ptr += calcsize(COLUMN_CODE_F)
            column_list.append(ColumnFactory.column_from_code(col_name, col_code))

        return cls(table_name, column_list)

    def _check_row_size(self) -> None:
        f = self.row_struct_format
        size = struct.calcsize(f)
        if size + 1 > BLOCK_SIZE:
            raise Exception('Row size more then block size.')

    def _check_ddl_size(self) -> None:
        if (struct.calcsize(self._get_ddl_struct_format()) + calcsize(TABLE_DDL_SIZE_F)) > BLOCK_SIZE:
            raise Exception(f"Table ddl more then block size")

    def _get_ddl_struct_format(self) -> str:
        struct_format = f'{TABLE_NAME_F}{COLUMN_COUNT_F}' + (f'{COLUMN_NAME_F}{COLUMN_CODE_F}' * self._column_count)
        return struct_format

    def __str__(self):
        column_list_str = '\n\t'.join(map(str, self._column_list))
        return f"{self._name}:\n\t{column_list_str}"

