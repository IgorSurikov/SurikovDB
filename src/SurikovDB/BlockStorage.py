import os
from typing import NoReturn, BinaryIO

from SurikovDB.Block import Block
from SurikovDB.constants import *


class BlockStorage:
    def __init__(self, path: str, block_size: int):
        self._block_size = block_size
        if not os.path.exists(path):
            f = open(path, 'x')
            f.close()

        self._path = path
        self._file = open(path, 'r+b', buffering=16 * self._block_size)

        file_size = os.path.getsize(path)

        if file_size % block_size:
            raise Exception('Incorrect BlockStorage file')
        else:
            self._block_count = file_size // self._block_size

    def read_block(self, index: int) -> Block:
        if index > self._block_count:
            raise Exception('Block is not exist')

        self._file.seek(index * self._block_size)
        block = Block(self._file.read(self._block_size), index, self._block_size)
        return block

    def write_block(self, block: Block) -> NoReturn:
        if len(block) != self._block_size:
            raise Exception('Block size is incorrect')

        if block.idx > self._block_count:
            raise Exception('Block is not exist')

        self._file.seek(block.idx * self._block_size)
        self._file.write(block)

    def allocate_block(self) -> Block:
        block = Block.empty(self._block_count, self._block_size)
        self._block_count += 1
        self.write_block(block)
        return block

    @property
    def block_count(self) -> int:
        return self._block_count

    @property
    def file(self) -> BinaryIO:
        return self._file

    @property
    def path(self) -> str:
        return self._path
