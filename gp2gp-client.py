#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#
import os
import logging
import optparse

from prettytable import PrettyTable
from gp2gp.client import GP2GPClient


def create_options():
    usage = "usage: %prog [options]"
    description = '''GP2GP Client Demo'''

    parser = optparse.OptionParser(
        description=description, prog='gp2gp-client', usage=usage)

    parser.add_option('-d', '--database', type="string",
                      dest="database", help="Connect to the database")

    parser.add_option('-H', '--host', type="string",
                      dest="host", help="Connect to the host, DEFAULT VALUE Env 'PGHOST'",
                      default=os.environ.get('PGHOST', '127.0.0.1'))

    parser.add_option('-p', '--port', type="string",
                      dest="port", help="Connect to the database on which port, DEFAULT VALUE Env 'PGPORT'",
                      default=os.environ.get('PGPORT', '5432'))

    parser.add_option('-u', '--user', type="string",
                      dest="user", help="username to connect the db")

    parser.add_option('-P', '--password', type="string",
                      dest="password", help="password to connect the db")

    parser.add_option('-c', '--query', type="string",
                      dest="query", help="the query which send to server")

    parser.add_option('-l', '--level', type="string",
                      dest="log_level", help="log level: info|debug", default="info")

    return parser


def output_result(columns, rows):
    table = PrettyTable(list(columns))
    for row in rows:
        table.add_row(row)
    print table


if __name__ == '__main__':
    parser = create_options()

    options, args = parser.parse_args()

    if options.log_level == 'debug':
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if not options.query:
        raise Exception("Not set the query!")

    queries = {
        "c1": options.query,
    }

    c = GP2GPClient(database=options.database,
                    user=options.user,
                    password=options.password,
                    queries=queries,
                    host=options.host,
                    port=options.port
                    )

    rows = c.get_data()
    output_result(c.columns, rows)
