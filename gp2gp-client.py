#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#
import os
import optparse

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
    return parser


if __name__ == '__main__':
    parser = create_options()

    options, args = parser.parse_args()

    queries = {
        "c1": "select * from t1",
    }

    c = GP2GPClient(database=options.database,
                    user=options.user,
                    password=options.password,
                    queries=queries,
                    host=options.host,
                    port=options.port
                    )

    result = c.get_data()

    print "Total Rows Count: %d" % len(result)
    print "All The Rows:"
    print result
