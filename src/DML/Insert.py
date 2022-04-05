from typing import Generator

from src.Block import TableMetaDataBlock, DataBlock, Block
from src.DML.InsertRow import InsertRow
from src.DML.Select import Select
from src.DataBaseCommand import DataBaseCommand
from src.DataBaseStorage import DataBaseStorage
from src.RowSet import RowSet
from src.constants import *


class Insert(DataBaseCommand):
    def __init__(self, table_name: str, select: Select):
        self._table_name = table_name
        self._select = select
        self._result = None

    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        select = self._select
        for modified_block in select.execute(data_base_storage):
            yield modified_block

        row_set: RowSet = select.result

        row_count = 0
        for row in row_set.row_gen:
            insert_row = InsertRow(self._table_name, row)
            for modified_block in insert_row.execute(data_base_storage):
                yield modified_block
            row_count += 1

        self._result = row_count

    @property
    def result(self):
        return self._result
