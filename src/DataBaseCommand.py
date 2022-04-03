import abc
from abc import ABCMeta
from typing import Generator, Any

from src.Block import Block
from src.DataBaseStorage import DataBaseStorage


class DataBaseCommand(metaclass=ABCMeta):

    @abc.abstractmethod
    def execute(self, data_base_storage: DataBaseStorage) -> Generator[Block, None, None]:
        pass

    @property
    @abc.abstractmethod
    def result(self) -> Any:
        pass
