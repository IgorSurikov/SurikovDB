import abc
from abc import ABCMeta
from struct import calcsize
from typing import Any

from jsonschema import Draft202012Validator

from SurikovDB.JSONQLException import JSONQLException
from SurikovDB.constants import *


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
    json_schema = {
        "minItems": 3,
        "maxItems": 3,
        "type": "array",
        "prefixItems": [
            {"type": "string"},
            {"enum": ["char"]},
            {"type": "number"}
        ],
    }

    @classmethod
    def from_json(cls, json: Any) -> 'ColumnChar':
        errors = list(Draft202012Validator(cls.json_schema).iter_errors(json))
        if errors:
            raise JSONQLException(errors)
        return cls(json[0], json[2])

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
    json_schema = {
        "minItems": 2,
        "maxItems": 2,
        "type": "array",
        "prefixItems": [
            {"type": "string"},
            {"enum": ["long"]}
        ],

    }

    @classmethod
    def from_json(cls, json: Any) -> 'ColumnLong':
        errors = list(Draft202012Validator(cls.json_schema).iter_errors(json))
        if errors:
            raise JSONQLException(errors)
        return cls(json[0])

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

    @staticmethod
    def column_from_json(json: Any) -> Column:
        if json[1] == 'char':
            return ColumnChar.from_json(json)
        elif json[1] == 'long':
            return ColumnLong.from_json(json)
        else:
            raise NotImplementedError
