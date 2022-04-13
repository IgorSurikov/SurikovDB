import json
import os
from typing import NoReturn
import shutil
from SurikovDB.constants import *
import platformdirs

from SurikovDB.DataBase import DataBase


class DBMS:
    data_base_list_file = os.path.join(platformdirs.user_data_dir('SurikovDB'), 'data_base_list.json')
    os.makedirs(os.path.dirname(data_base_list_file), exist_ok=True)

    if not os.path.exists(data_base_list_file):
        with open(data_base_list_file, 'x') as f:
            json.dump([], f)

    @staticmethod
    def get_data_base(name):
        data_base_list = DBMS.get_data_base_list()

        for i in data_base_list:
            if i['name'] == name:
                data_base = i
                break
        else:
            raise Exception('This database is not exists')
        return DataBase(os.path.join(data_base['path'], name))

    @staticmethod
    def create_data_base(path: str, name: str) -> DataBase:
        with open(DBMS.data_base_list_file, 'r') as f:
            data_base_list = json.load(f)

        db_path = os.path.join(path, name)

        if name in [os.path.split(path)[-1] for path in data_base_list]:
            raise Exception('Database name must be unique')

        os.mkdir(db_path)
        data_base_list.append(db_path)

        with open(DBMS.data_base_list_file, 'w') as f:
            json.dump(data_base_list, f)

        return DataBase(os.path.join(db_path, name))

    @staticmethod
    def drop_data_base(name: str) -> NoReturn:
        data_base_list = DBMS.get_data_base_list()

        for i in data_base_list:
            if i['name'] == name:
                data_base = i
                break
        else:
            raise Exception('This database is not exists')

        if os.path.exists(data_base['path']):
            shutil.rmtree(data_base['path'])

        data_base_list = [i['path'] for i in data_base_list if i['name'] != name]
        with open(DBMS.data_base_list_file, 'w') as f:
            json.dump(data_base_list, f)

    @staticmethod
    def get_data_base_list() -> list[dict]:
        with open(DBMS.data_base_list_file, 'r') as f:
            data_base_list = json.load(f)

        return [{'name': os.path.split(path)[-1], 'path': path} for path in data_base_list]

