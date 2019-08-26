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

    parser.add_option('-p', '--ports', type="string",
                      dest="ports", help="All segment ports joined by commas(no space), e.g. 25432,25433,25434. DEFAULT VALUE Env 'PGPORT'",
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

    # parse the port string into a list
    ports = options.ports.split(",")

    for port in ports:
        # TODO: async retrieve all segments of this machine.
        pass

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
    # TODO: whether print or not(test mode)
