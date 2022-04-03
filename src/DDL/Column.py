import abc
from abc import ABCMeta
from struct import calcsize
from typing import Any

from src.constants import *


class Column(metaclass=ABCMeta):

    def __init__(self, name: str):
        if len(name.encode()) > calcsize(COLUMN_NAME_F):
            raise Exception(f"Column name '{name}' more then column name max size({calcsize(COLUMN_NAME_F)})")
        self._name = name

    @property
    def name(self) -> str:
        return self._name

    @property
    @abc.abstractmethod
    def struct_format(self) -> str:
        pass

    @property
    @abc.abstractmethod
    def code(self) -> int:
        pass

    @abc.abstractmethod
    def is_valid_value(self, value: Any) -> bool:
        pass


class ColumnChar(Column):
    def __init__(self, name: str, len_bytes: int):
        super().__init__(name)
        self._struct_format = 's'

        if len_bytes < 1:
            raise Exception('Incorrect char length')

        self._len_bytes = len_bytes

    def is_valid_value(self, value: Any) -> bool:
        if isinstance(value, str) and len(value.encode()) <= self._len_bytes:
            return True
        else:
            return False

    @property
    def struct_format(self) -> str:
        return f'{self._len_bytes}{self._struct_format}'

    @property
    def code(self) -> int:
        return self._len_bytes

    def __str__(self):
        return f'{self.name} - char({self._len_bytes})'


class ColumnLong(Column):

    def __init__(self, name: str):
        super().__init__(name)
        self._struct_format = 'l'

    def is_valid_value(self, value: Any) -> bool:
        if isinstance(value, int) and (-2147483648 <= value <= 2147483647):
            return True
        else:
            return False

    @property
    def struct_format(self) -> str:
        return self._struct_format

    @property
    def code(self) -> int:
        return -1

    def __str__(self):
        return f'{self.name} - long'


class ColumnFactory:

    @staticmethod
    def column_from_code(name: str, code: int) -> Column:
        if code > 0:
            return ColumnChar(name, code)
        elif code == -1:
            return ColumnLong(name)
        else:
            raise Exception('Incorrect code')
