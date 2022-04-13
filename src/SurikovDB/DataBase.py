import os
from time import time
from typing import Union

from SurikovDB.DataBaseStorage import DataBaseStorage
from SurikovDB.JSONQLParser import JSONQLParser
from SurikovDB.RowSet import RowSet
from SurikovDB.Server.server import DataBaseServer
from SurikovDB.Transaction import Transaction
from SurikovDB.constants import *


class DataBase:
    def __init__(self, path: str):
        self._data_base_storage = DataBaseStorage(path)
        self._name = os.path.split(path)[-1]
        self._path = path
        # self._recover_data()

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

    def _recover_data(self):
        data_base_dir_path = os.path.join(*os.path.split(self._path)[0:-1])
        transaction_file_list = [i for i in os.listdir(data_base_dir_path) if i.startswith('tlog-')]

        transaction_file_list = sorted(transaction_file_list, key=lambda i: float(i[5:]), reverse=True)

        for transaction_file in transaction_file_list:
            transaction_file_path = os.path.join(data_base_dir_path, transaction_file)
            transaction = Transaction.from_transaction_file_path(self._data_base_storage, transaction_file_path)
            transaction.rollback()

    def __del__(self):
        self._data_base_storage.file.close()

    def run(self, host=None, port=None):
        dbs = DataBaseServer(self, host=host, port=port)
        dbs.run()

    @property
    def name(self) -> str:
        return self._name

    @property
    def path(self) -> str:
        return self._path

    @property
    def data_base_storage(self):
        return self._data_base_storage
