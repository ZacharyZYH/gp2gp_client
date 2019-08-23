#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#
import os
import logging
import optparse
import psycopg2

def create_options():
    usage = "usage: %prog [options]"
    description = '''GP2GP Retrieve Client Demo'''

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

    parser.add_option('-t', '--token', type="string",
                      dest="token", help="password to connect the db")

    parser.add_option('-l', '--level', type="string",
                      dest="log_level", help="log level: info|debug", default="info")

    return parser


if __name__ == '__main__':
    parser = create_options()

    options, args = parser.parse_args()

    if options.log_level == 'debug':
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # TODO: retrieve from the specified host

    conn = psycopg2.connect(
                database=options.database,
                user=options.user,
                password=options.token,
                host=options.host,
                port=options.port,
                options="-c gp_session_role=retrieve"
            )
    cursor = conn.cursor()
    cursor.execute('retrieve all from "%s"' % options.token)
    rows = cursor.fetchall()
    print(rows)

# connect in retrieve mode
# declare cursor
# execute "RETRIEVE ..." 
# fecthall