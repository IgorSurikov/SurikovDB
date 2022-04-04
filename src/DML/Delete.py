from typing import Generator

from src.Block import Block
from src.DataBaseCommand import DataBaseCommand
from src.DML.Update import Update
from src.DataBaseStorage import DataBaseStorage
from src.Expression import Expression


class Delete(DataBaseCommand):
    def __init__(self, table_name: str, filter_exp: Expression):
        self._table_name = table_name
        self._filter_exp = filter_exp
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        update_map = {'.ROW_IS_DELETED': Expression(True)}

        update = Update(self._table_name, self._filter_exp, update_map)

        for modified_block in update.execute(data_base_storage):
            yield modified_block

        self._result = update.result

    @property
    def result(self):
        return self._result
