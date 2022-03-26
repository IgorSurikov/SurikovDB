from typing import Generator

from src.Block import TableMetaDataBlock, DataBlock, Block
from src.DataBaseStorage import DataBaseStorage
from src.Expression import Expression
from src.constants import *


class Update:
    def __init__(self, table_name: str, filter_exp: Expression, update_map: dict[str, Expression]):
        self._table_name = table_name
        self._filter_exp = filter_exp
        self._update_map = update_map
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage):
        table_meta_data, block_index = data_base_storage.get_table_meta_data(self._table_name)

        column_map = {
            c.name: i + 1 for i, c in enumerate(table_meta_data.column_list)
        }
        column_map['.ROW_IS_DELETED'] = 0

        for data_block in data_base_storage.scan_table_data_block_gen(table_meta_data, block_index):
            row_list = data_block.read_rows()
            filter_exp_func, filter_exp_arg_name_list = self._filter_exp.parse()

            if not filter_exp_arg_name_list.issubset(column_map.keys()):
                raise Exception(
                    f'columns: {filter_exp_arg_name_list - set(column_map.keys())} is not exist in {self._table_name}')

            row_list_filtered = []
            for row_id, row in enumerate(row_list):
                filter_exp_arg = {i: row[column_map[i]] for i in filter_exp_arg_name_list}
                if not filter_exp_func(**filter_exp_arg):
                    continue

                row_list_filtered.append((row_id, row))

            if len(row_list_filtered) == 0:
                continue

            row_list_result = []
            for row_id, row in row_list_filtered:
                for column_name, update_exp in self._update_map.items():
                    update_exp_func, update_exp_arg_name_list = update_exp.parse()

                    if not update_exp_arg_name_list.issubset(column_map.keys()):
                        raise Exception(
                            f'columns: {filter_exp_arg_name_list - set(column_map.keys())} is not exist in {self._table_name}')

                    update_exp_arg = {i: row[column_map[i]] for i in update_exp_arg_name_list}
                    row = list(row)
                    row[column_map[column_name]] = update_exp_func(**update_exp_arg)

                for v, c in zip(row[1:], table_meta_data.column_list):
                    if not c.is_valid_value(v):
                        raise Exception(f'Column {c.name} - value {v} is not valid')

                row_list_result.append((row_id, row))

            yield data_block
            for row_id, row in row_list_result:
                data_block.write_row(row, row_id)

            data_base_storage.write_block(data_block)

    @property
    def result(self):
        return self._result
