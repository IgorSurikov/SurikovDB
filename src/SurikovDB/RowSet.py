from typing import Generator, Callable

from SurikovDB.Expression import Expression
from SurikovDB.constants import *


class RowSet:
    def __init__(self, column_name_list: list[str], row_gen: Callable[[], Generator[ROW_TYPE, None, None]], info: list):
        self._column_name_list = column_name_list
        self._row_gen = row_gen
        self._info = info

    def where(self, filter_exp: Expression) -> 'RowSet':
        filter_exp_func, filter_exp_arg_name_list = filter_exp.parse()

        def row_gen() -> Generator[ROW_TYPE, None, None]:
            for row in self.row_gen:
                row_map = self._row_map(row)
                if filter_exp_func(**row_map):
                    yield row

        info = self._info + [f'where: {filter_exp.value}']
        return RowSet(self.column_name_list, row_gen, info)

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

        info = self._info + [f'select: {[exp.value for exp in exp_list]}']
        return RowSet(column_name_list, row_gen, info)

    def join(self, row_set: 'RowSet', on_exp: Expression) -> 'RowSet':

        on_exp_func, on_exp_arg_name_list = on_exp.parse()
        row_set_source_table = row_set.info[0]
        source_table = self.info[0]
        column_name_list = [f'{source_table}.{c}' if '.' not in c else c for c in self._column_name_list] + \
                           [f'{row_set_source_table}.{c}' if '.' not in c else c for c in row_set.column_name_list]

        def row_gen() -> Generator[ROW_TYPE, None, None]:
            for row1 in self.row_gen:
                for row2 in row_set.row_gen:
                    row = row1 + row2
                    row_map = self._row_map(row, column_name_list)
                    if on_exp_func(**row_map):
                        yield row

        info = self._info + [f'join: {row_set.info} on {on_exp.value}']
        return RowSet(column_name_list, row_gen, info)

    def _row_map(self, row: ROW_TYPE, column_name_list: list[str] = None) -> dict[str, DB_TYPE]:
        if column_name_list is None:
            column_name_list = self._column_name_list
        return {c: v for c, v in zip(column_name_list, row)}

    @property
    def column_name_list(self) -> list[str]:
        return self._column_name_list

    @property
    def row_gen(self) -> Generator[ROW_TYPE, None, None]:
        return self._row_gen()

    @property
    def info(self):
        return self._info

    @property
    def json(self):
        return {
            'columns': self.column_name_list,
            'data': [row for row in self.row_gen]
        }
