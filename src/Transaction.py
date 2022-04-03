import datetime
import os
from struct import unpack_from, pack, calcsize
from typing import NoReturn, Generator

from src.Block import Block
from src.BlockStorage import BlockStorage
from src.DataBaseCommand import DataBaseCommand
from src.DataBaseStorage import DataBaseStorage
from src.constants import *


class Transaction(BlockStorage):

    def __init__(self, command_list: list[DataBaseCommand], path: str = None):
        if path is None:
            path = 'tlog-' + str(datetime.datetime.now().timestamp())
        super().__init__(path, BLOCK_SIZE + calcsize(POINTER_F))

        self._command_list = command_list
        self._result = None
        self._block_idx_list = [i.idx for i in self._block_gen()]
        self._path = path

    def __del__(self):
        self._file.close()
        os.remove(self._path)

    @property
    def result(self):
        return self._result

    def _save_database_storage_block(self, block: Block) -> NoReturn:
        if block.idx in self._block_idx_list:
            return

        block_target = self.allocate_block()
        block_index_encode = pack(f'{POINTER_F}', block.idx)
        block_target.override(0, block_index_encode)
        block_target.override(calcsize(f'{POINTER_F}'), bytes(block))
        self.write_block(block_target)
        self._block_idx_list.append(block.idx)

    def _block_gen(self) -> Generator[Block, None, None]:
        for i in range(self.block_count):
            block = self.read_block(i)
            database_storage_block_data = block[calcsize(f'{POINTER_F}'):]
            database_storage_block_idx = unpack_from(f'{POINTER_F}', block)[0]
            yield Block(database_storage_block_data, database_storage_block_idx, BLOCK_SIZE)

    def rollback(self, data_base_storage: DataBaseStorage) -> NoReturn:
        for i in self._block_gen():
            data_base_storage.write_block(i)

    def execute(self, data_base_storage: DataBaseStorage) -> NoReturn:
        for c in self._command_list:
            for modified_block in c.execute(data_base_storage):
                self._save_database_storage_block(modified_block)

        self._result = self._command_list[::-1][0].result
