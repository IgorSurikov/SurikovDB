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

        if not filter_exp_arg_name_list.issubset(self._column_name_list):
            raise Exception(
                f'columns: {filter_exp_arg_name_list - set(self._column_name_list)} is not exist in RowSet')

        def row_gen() -> Generator[ROW_TYPE, None, None]:
            for row in self.row_gen:
                row_map = self._row_map(row)
                if filter_exp_func(**row_map):
                    yield row

        return RowSet(self.column_name_list, row_gen())

    def select(self, exp_list: list[Union[Expression, str]]) -> 'RowSet':

        j = 0
        column_name_list = []
        for exp in exp_list:
            if isinstance(exp, str):
                column_name_list.append(exp)

            elif isinstance(exp, Expression):
                column_name_list.append(f'col_{j}')
                j += 1

        def row_gen() -> Generator[ROW_TYPE, None, None]:
            for row in self.row_gen:
                row_map = self._row_map(row)
                row_result = []
                for e in exp_list:
                    if isinstance(e, str):
                        row_result.append(row_map[e])
                    if isinstance(e,Expression):
                        exp_func, exp_arg_name_list = e.parse()
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
