import argparse

from SurikovDB.DBMS import DBMS

parser = argparse.ArgumentParser(description="SurikovDB DBMS", prog='SurikovDB')
subparsers = parser.add_subparsers()


def list_db(args):
    db_list = DBMS.get_data_base_list()
    for i in db_list:
        print(f"Name: {i['name']}, path: {i['path']}")


def create_db(args):
    DBMS.create_data_base(args.path, args.database_name)


def drop_db(args):
    DBMS.drop_data_base(args.database_name)


def start_db(args):
    db = DBMS.get_data_base(args.database_name)
    host = args.host[0] if args.host else args.host
    port = args.port[0] if args.port else args.port
    db.run(host, port)


list_db_parser = subparsers.add_parser('list_db', help='List all databases')
list_db_parser.set_defaults(func=list_db)

create_db_parser = subparsers.add_parser('create_db', help='Create database')
create_db_parser.add_argument('database_name', help='Database name')
create_db_parser.add_argument('path', help='Path to folder to store database')
create_db_parser.set_defaults(func=create_db)

drop_db_parser = subparsers.add_parser('drop_db', help='Drop database')
drop_db_parser.add_argument('database_name', help='Database name')
drop_db_parser.set_defaults(func=drop_db)

start_db_parser = subparsers.add_parser('start_db', help='Start database http server')
start_db_parser.add_argument('database_name', help='Database name')
start_db_parser.add_argument('-H', dest='host', help='Host', nargs=1, default=None)
start_db_parser.add_argument('-P', dest='port', help='Port', nargs=1, default=None)
start_db_parser.set_defaults(func=start_db)


def main():
    args = parser.parse_args()
    args.func(args)

