import argparse

from SurikovDB.DBMS import DBMS

parser = argparse.ArgumentParser(description="SurikovDB DBMS", prog='SurikovDB')
subparsers = parser.add_subparsers()


def list_db(args):
    db_list = DBMS.get_data_base_list()
    for i in db_list:
        print(f"Name: {i['name']}, path: {i['path']}")


def create_db(args):
    pass


def drop_db(args):
    pass


def start_db(args):
    pass


list_db_parser = subparsers.add_parser('list_db', help='List all databases')
list_db_parser.set_defaults(func=list_db)

create_db_parser = subparsers.add_parser('create_db', help='Create database')
create_db_parser.add_argument('database_name', help='Database name')

drop_db_parser = subparsers.add_parser('drop_db', help='Drop database')
drop_db_parser.add_argument('database_name', help='Database name')

start_db_parser = subparsers.add_parser('start_db', help='Start database http server')
start_db_parser.add_argument('database_name', help='Database name')
start_db_parser.add_argument('-H', help='Host', nargs=1, default='localhost')
start_db_parser.add_argument('-P', help='Port', nargs=1, default='3471')


def main():
    args = parser.parse_args()
    args.func(args)
