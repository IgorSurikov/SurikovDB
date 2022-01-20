import struct

from Column import Column
from constants import BLOCK_SIZE


class Table:
    def __init__(self, name: str, column_list: list[Column]):
        self._name = name
        self._column_list = column_list
        self._check_row_size()  # throw exception

    def get_struct_pack_format(self):
        return ''.join([c.get_struct_format() for c in self._column_list])

    def _check_row_size(self) -> None:
        f = self.get_struct_pack_format()
        size = struct.calcsize(f)
        if size + 1 > BLOCK_SIZE:
            raise Exception('Row size must be less then block size.')