from typing import Any, Union

from jsonschema import Draft202012Validator

from src.DDL.CreateTable import CreateTable
from src.DDL.DropTable import DropTable
from src.DML.Delete import Delete
from src.DML.Insert import Insert
from src.DML.InsertRows import InsertRows
from src.DML.Select import Select
from src.DML.Update import Update
from src.JSONQLException import JSONQLException
from src.Transaction import Transaction


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
    def parse(json: Union[dict, list]) -> Transaction:
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

        return Transaction(command_list)
