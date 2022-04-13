from typing import Union

from jsonschema import Draft202012Validator

from SurikovDB.DataBaseCommand import DataBaseCommand
from SurikovDB.DDL.CreateTable import CreateTable
from SurikovDB.DDL.DropTable import DropTable
from SurikovDB.DML.Delete import Delete
from SurikovDB.DML.Insert import Insert
from SurikovDB.DML.InsertRows import InsertRows
from SurikovDB.DML.Select import Select
from SurikovDB.DML.Update import Update
from SurikovDB.JSONQLException import JSONQLException
from SurikovDB.constants import *

class JSONQLParser:
    json_schema = {
        "type": "object",
        "properties": {
            "type": {
                "enum": ["create_table", "drop_table", "select", "insert_rows", "insert", "delete", "update"]
            }
        },
        "required": ["type"]

    }

    @staticmethod
    def parse(json: Union[dict, list]) -> list[DataBaseCommand]:
        command_list = []
        command_list_json = []
        if isinstance(json, list):
            command_list_json = json
        else:
            command_list_json.append(json)

        for c_json in command_list_json:
            errors = list(Draft202012Validator(JSONQLParser.json_schema).iter_errors(c_json))
            if errors:
                raise JSONQLException(errors)

            command_type = c_json['type']

            command_type_map = {
                'create_table': CreateTable,
                'drop_table': DropTable,
                'select': Select,
                'insert_rows': InsertRows,
                'insert': Insert,
                'delete': Delete,
                'update': Update,
            }

            command = command_type_map[command_type].from_json(c_json)
            command_list.append(command)

        return command_list
