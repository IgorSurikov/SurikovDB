import os
from time import time
from typing import Union

from src.SurikovDB.DataBaseStorage import DataBaseStorage
from src.SurikovDB.JSONQLParser import JSONQLParser
from src.SurikovDB.RowSet import RowSet
from src.SurikovDB.Transaction import Transaction


class DataBase:
    def __init__(self, path: str):
        self._data_base_storage = DataBaseStorage(path)
        self._name = os.path.split(path)[-1]
        self._path = path

    def query(self, json: Union[dict, list]) -> dict:
        start_time = time()
        result = {}
        try:
            command_list = JSONQLParser.parse(json)
            tr = Transaction(command_list, self._data_base_storage)
            try:
                tr.execute()
            except Exception as e:
                tr.rollback()
                raise e

            result = tr.result

            if isinstance(result, RowSet):
                result = result.json

            result = {
                'result': result,
            }

        except Exception as e:
            result = {
                'error': str(e)
            }

        finally:
            end_time = time()
            result['execution_time'] = end_time - start_time
            return result

    def get_table_list(self):
        table_list = []
        for i in self._data_base_storage.table_meta_data_gen():
            table_list.append(i.table_meta_data.json)
        return table_list

    def __del__(self):
        self._data_base_storage.file.close()

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> str:
        return self._path

    @property
    def data_base_storage(self):
        return self._data_base_storage