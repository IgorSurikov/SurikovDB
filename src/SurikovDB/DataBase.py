import os
from time import time
from typing import Union

from src.SurikovDB.DataBaseStorage import DataBaseStorage
from src.SurikovDB.JSONQLParser import JSONQLParser
from src.SurikovDB.RowSet import RowSet


class DataBase:
    def __init__(self, path: str):
        self._data_base_storage = DataBaseStorage(path)
        self._name = os.path.split(path)[-1]

    def query(self, json: Union[dict, list]) -> dict:
        start_time = time()
        result = {}
        try:
            tr = JSONQLParser.parse(json)
            try:
                tr.execute(self._data_base_storage)
            except Exception as e:
                tr.rollback(self._data_base_storage)
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
            table_list.append(str(i.table_meta_data))
        return table_list

    def __del__(self):
        self._data_base_storage.file.close()


    @property
    def name(self) -> str:
        return self._name
