import abc
from abc import ABCMeta


class Column(metaclass=ABCMeta):

    def __init__(self, name):
        self._name = name

    @property
    def name(self):
        return self._name

    @abc.abstractmethod
    def get_struct_format(self) -> str:
        pass


class ColumnChar(Column):
    def __init__(self, name, len_bytes: int):
        super().__init__(name)
        self._pack_format = 'l'
        self._len_bytes = len_bytes

    def get_struct_format(self) -> str:
        return f'{self._len_bytes}s'


class ColumnLong(Column):

    def __init__(self, name):
        super().__init__(name)
        self.pack_format = 'l'

    def get_struct_format(self) -> str:
        return self.pack_format
