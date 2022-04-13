import datetime
import os
from struct import unpack_from, pack, calcsize
from typing import NoReturn, Generator, Optional

from SurikovDB import DataBaseStorage
from SurikovDB.Block import Block
from SurikovDB.BlockStorage import BlockStorage
from SurikovDB.DataBaseCommand import DataBaseCommand
from SurikovDB.constants import *


class Transaction(BlockStorage):

    def __init__(self, command_list: list[DataBaseCommand], data_base_storage: DataBaseStorage,
                 transaction_file_path: Optional[str] = None):

        if transaction_file_path is None:
            data_base_dir = os.path.split(data_base_storage.path)[0:-1]
            transaction_file_name = 'tlog-' + str(datetime.datetime.now().timestamp())
            transaction_file_path = os.path.join(*data_base_dir, transaction_file_name)

        super().__init__(transaction_file_path, BLOCK_SIZE + calcsize(POINTER_F))

        self._command_list = command_list
        self._result = None
        self._block_idx_list = [i.idx for i in self._block_gen()]
        self._path = transaction_file_path
        self._data_base_storage = data_base_storage

    @classmethod
    def from_transaction_file_path(cls, data_base_storage: DataBaseStorage, transaction_file_path: str):
        return cls([], data_base_storage, transaction_file_path)

    def __del__(self):
        self.file.close()
        os.remove(self.path)

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

    def rollback(self) -> NoReturn:
        for i in self._block_gen():
            self._data_base_storage.write_block(i)

    def execute(self) -> NoReturn:

        for c in self._command_list:
            for modified_block in c.execute(self._data_base_storage):
                self._save_database_storage_block(modified_block)

        self._result = self._command_list[::-1][0].result
