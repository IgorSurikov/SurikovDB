from typing import Generator

from src.Column import Column
from src.constants import *


class RowSet:
    def __init__(self, column_list: list[Column], row_gen: Generator[ROW_TYPE, None, None]):
        self._column_list = column_list
        self._row_gen = row_gen

    @property
    def column_list(self) -> list[Column]:
        return self._column_list

    @property
    def row_gen(self) -> Generator[ROW_TYPE, None, None]:
        return self._row_gen
