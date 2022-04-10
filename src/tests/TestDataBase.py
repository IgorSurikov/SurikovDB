from unittest import TestCase

from src.SurikovDB.DBMS import DBMS


class TestDataBase(TestCase):

    def setUp(self) -> None:
        DBMS.create_data_base(r'D:\Dev\python\SurikovDB', 'TestDB')
        self.data_base = DBMS.get_data_base('TestDB')

    def test_data_base_query(self):
        r = self.data_base.query({
            "type": "create_table",
            "table_name": "UserInfo",
            "columns": [
                ["id", "long"],
                ["name", "char", 20],
                ["address", "char", 50]
            ]
        })

        self.assertIsNone(r['result'])
        self.assertIn({'table_name': 'UserInfo', 'columns': ['id - long', 'name - char(20)', 'address - char(50)']},
                      self.data_base.get_table_list())

        r = self.data_base.query({
            "type": "create_table",
            "table_name": "Role",
            "columns": [
                ["id", "long"],
                ["role_name", "char", 20],
            ]
        })

        self.assertIsNone(r['result'])

        r = self.data_base.query({
            "type": "create_table",
            "table_name": "User2Role",
            "columns": [
                ["role_id", "long"],
                ["user_id", "long"],
                ["is_active", "long"],
            ]
        })

        self.assertIsNone(r['result'])
        self.assertEqual(len(self.data_base.get_table_list()), 3)

        r = self.data_base.query({
            "type": "insert_rows",
            "table_name": "UserInfo",
            "rows": [
                [1, 'Игорь', 'Восточная 39'],
                [2, 'Алёна', 'Рафиева 82'],
                [3, 'Макс', 'Пушкина 14'],
                [4, 'Ошибка', 'Ошибка'],
            ]
        })

        self.assertEqual(r['result'], 4)

        r = self.data_base.query({
            "type": "select",
            "select": ['*'],
            "from": "UserInfo"
        })

        self.assertEqual(r['result']['data'], [(1, 'Игорь', 'Восточная 39'),
                                               (2, 'Алёна', 'Рафиева 82'),
                                               (3, 'Макс', 'Пушкина 14'),
                                               (4, 'Ошибка', 'Ошибка')])

        r = self.data_base.query({
            "type": "insert_rows",
            "table_name": "Role",
            "rows": [
                [1, 'Админ'],
                [2, 'Юзер'],
                [3, 'Модератор'],
            ]
        })

        self.assertEqual(r['result'], 3)

        r = self.data_base.query({
            "type": "insert_rows",
            "table_name": "User2Role",
            "rows": [
                [1, 1, 1],
                [2, 1, 1],
                [3, 2, 1],
                [2, 2, 1],
                [2, 3, 1],
                [1, 3, 0],
            ]
        })

        self.assertEqual(r['result'], 6)

        r = self.data_base.query({
            "type": "update",
            "table_name": "UserInfo",
            "set": {
                "address": "'Пушкина 15'",
            },
            "where": ["eq", "id", 3]
        })

        self.assertEqual(r['result'], 1)

        r = self.data_base.query({
            "type": "select",
            "select": ['*'],
            "from": "UserInfo"
        })

        self.assertEqual(r['result']['data'], [(1, 'Игорь', 'Восточная 39'),
                                               (2, 'Алёна', 'Рафиева 82'),
                                               (3, 'Макс', 'Пушкина 15'),
                                               (4, 'Ошибка', 'Ошибка')])

        r = self.data_base.query({
            "type": "delete",
            "table_name": "UserInfo",
            "where": ["eq", "name", "'Ошибка'"]
        })

        self.assertEqual(r['result'], 1)

        r = self.data_base.query({
            "type": "select",
            "select": ['*'],
            "from": "UserInfo"
        })

        self.assertEqual(r['result']['data'], [(1, 'Игорь', 'Восточная 39'),
                                               (2, 'Алёна', 'Рафиева 82'),
                                               (3, 'Макс', 'Пушкина 15')])

        r = self.data_base.query({
            "type": "select",
            "select": ['*'],
            "from": "UserInfo",
            "join": {
                "User2Role": ["eq", "UserInfo.id", "User2Role.user_id"],
                "Role": ["eq", "Role.id", "User2Role.role_id"],
            },
            "where": ['eq', 'User2Role.is_active', 1]
        })

        self.assertEqual(r['result']['columns'], ['UserInfo.id',
                                                  'UserInfo.name',
                                                  'UserInfo.address',
                                                  'User2Role.role_id',
                                                  'User2Role.user_id',
                                                  'User2Role.is_active',
                                                  'Role.id',
                                                  'Role.role_name'])
        self.assertEqual(r['result']['data'], [(1, 'Игорь', 'Восточная 39', 1, 1, 1, 1, 'Админ'),
                                               (1, 'Игорь', 'Восточная 39', 2, 1, 1, 2, 'Юзер'),
                                               (2, 'Алёна', 'Рафиева 82', 3, 2, 1, 3, 'Модератор'),
                                               (2, 'Алёна', 'Рафиева 82', 2, 2, 1, 2, 'Юзер'),
                                               (3, 'Макс', 'Пушкина 15', 2, 3, 1, 2, 'Юзер')])

        r = self.data_base.query({
            "type": "create_table",
            "table_name": "UserRoleInfo",
            "columns": [
                ["user_name", "char", 20],
                ["role_name", "char", 20],
                ["desc", "char", 100],
            ]
        })

        self.assertIsNone(r['result'])

        r = self.data_base.query({
            "type": "insert",
            "table_name": "UserRoleInfo",
            "select": {
                "type": "select",
                "select": ['UserInfo.name',
                           'Role.role_name',
                           ['add', "UserInfo.name", ['add', "' имеет роль - '", "Role.role_name"]]],
                "from": "UserInfo",
                "join": {
                    "User2Role": ["eq", "UserInfo.id", "User2Role.user_id"],
                    "Role": ["eq", "Role.id", "User2Role.role_id"],
                },
                "where": ['eq', 'User2Role.is_active', 1]
            }})

        self.assertEqual(r['result'], 5)

        r = self.data_base.query({
            "type": "select",
            "select": ['*'],
            "from": "UserRoleInfo"
        })

        self.assertEqual(r['result']['columns'], ['user_name', 'role_name', 'desc'])
        self.assertEqual(r['result']['data'], [('Игорь', 'Админ', 'Игорь имеет роль - Админ'),
                                               ('Игорь', 'Юзер', 'Игорь имеет роль - Юзер'),
                                               ('Алёна', 'Модератор', 'Алёна имеет роль - Модератор'),
                                               ('Алёна', 'Юзер', 'Алёна имеет роль - Юзер'),
                                               ('Макс', 'Юзер', 'Макс имеет роль - Юзер')])

    def tearDown(self) -> None:
        del self.data_base
        DBMS.drop_data_base('TestDB')
