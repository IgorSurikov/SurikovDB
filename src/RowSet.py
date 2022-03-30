from typing import Generator

from src.Column import Column
from src.Expression import Expression
from src.constants import *


class RowSet:
    def __init__(self, column_name_list: list[str], row_gen: Generator[ROW_TYPE, None, None]):
        self._column_name_list = column_name_list
        self._row_gen = row_gen

    def where(self, filter_exp: Expression) -> 'RowSet':
        filter_exp_func, filter_exp_arg_name_list = filter_exp.parse()

        def row_gen() -> Generator[ROW_TYPE, None, None]:
            for row in self.row_gen:
                row_map = self._row_map(row)
                if filter_exp_func(**row_map):
                    yield row

        return RowSet(self.column_name_list, row_gen())

    def select(self, exp_list: list[Expression]) -> 'RowSet':

        exp_func_list = [exp.parse()[0] for exp in exp_list]

        j = 0
        column_name_list = []
        for exp in exp_list:
            if exp.value in self._column_name_list:
                column_name_list.append(exp.value)
            else:
                column_name_list.append(f'col_{j}')
                j += 1

        def row_gen() -> Generator[ROW_TYPE, None, None]:
            for row in self.row_gen:
                row_map = self._row_map(row)
                row_result = []
                for exp_func in exp_func_list:
                    row_result.append(exp_func(**row_map))
                yield tuple(row_result)

        return RowSet(column_name_list, row_gen())

    def _row_map(self, row: ROW_TYPE) -> dict[str, DB_TYPE]:
        return {c: v for c, v in zip(self._column_name_list, row)}

    @property
    def column_name_list(self) -> list[str]:
        return self._column_name_list

    @property
    def row_gen(self) -> Generator[ROW_TYPE, None, None]:
        return self._row_gen
