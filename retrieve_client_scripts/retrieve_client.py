#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#
import logging
import multiprocessing
import optparse
import os
from time import sleep

import psycopg2


def create_options():
    usage = "usage: %prog [options]"
    description = '''GP2GP Retrieve Client Demo'''

    parser = optparse.OptionParser(
        description=description, prog='retrieve_client.py', usage=usage)

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

    parser.add_option('-s', '--size', type="string",
                      dest="fetch_size", help="lines per retrieve")

    parser.add_option('-l', '--level', type="string",
                      dest="log_level", help="log level: info|debug", default="info")

    # parser.add_option('-T', '--test', action="store_true",
    #                   dest="perf_test", help="execute for performance testing(not generating result)", default=False)

    return parser

def retrieve_one(options, port):
    try:
        conn = psycopg2.connect(
                database=options.database,
                user=options.user,
                password=options.token,
                host=options.host,
                port=port,
                options="-c gp_session_role=retrieve"
            )
        cursor = conn.cursor()
        retrieve_sql = 'retrieve ' + options.fetch_size + ' from "%s"' % options.token
        while True:
            cursor.execute(retrieve_sql)
            rows = cursor.fetchall()
            if not rows:
                break
        conn.close()
        if not options.perf_test:
            print(rows)

    except Exception as e:
        # if mutiple retrieve process finish retrieving at the same time,
        # they might still try to retrieve after the cursor is no longer in EXECUTED status
        if e.message.startswith('the PARALLEL CURSOR related to endpoint token ' 
                                + options.token
                                + ' is not EXECUTED'):
            pass
        else:
            logging.info(e)

if __name__ == '__main__':
    parser = create_options()
    options, args = parser.parse_args()

    if options.log_level == 'debug':
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # parse the port string into a list
    ports = options.ports.split(",")

    pool = []
    for port in ports:
        proc = multiprocessing.Process(target=retrieve_one, args=(options, port))
        proc.start()
        pool.append(proc)

    map(lambda p: p.join(), pool)
