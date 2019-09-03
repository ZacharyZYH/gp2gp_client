#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#
import logging
import optparse
import os
import subprocess
import time

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

    parser.add_option('-q', '--query', type="string",
                      dest="query", help="the query which send to server")

    parser.add_option('-f', '--file', type="string",
                      dest="filename", help="the file that stores the query")

    parser.add_option('-n', '--normal', action="store_true",
                      dest="is_normal", help="use normal cursor instead of parallel cursor", default=False)

    parser.add_option('-l', '--level', type="string",
                      dest="log_level", help="log level: info|debug", default="info")

    parser.add_option('-t', '--test', action="store_true",
                      dest="perf_test", help="not generating the result, just for performance testing", default=False)

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

    if not options.query and not options.filename:
        raise Exception("Not set the query nor specify a file!")

    if options.query and options.filename:
        raise Exception("Cannot set the query and specify a file at the same time")

    if options.query:
        queries = {
            "c1": options.query,
        }
    else:
        f = open(options.filename)
        queries = {
            "c1": f.read(),
        }
        f.close()

    c = GP2GPClient(database=options.database,
                    user=options.user,
                    password=options.password,
                    queries=queries,
                    host=options.host,
                    port=options.port,
                    is_normal=options.is_normal,
                    perf_test=options.perf_test
                    )

    # clear memory cache
    if options.perf_test:
        hosts = c.get_hosts()
        print("Hosts: ", hosts)
        for host in hosts:
            print("cleaning cache on " + host[0] + "...")
            args = ["ssh","root@" + host[0],"echo", "3", ">",  "/proc/sys/vm/drop_caches"]
            subprocess.call(args)
            print("finished cleaning cache on " + host[0])
        print("Now running the query")

    time_start=time.time()
    rows = c.get_data()
    time_end=time.time()

    if not options.perf_test:
        output_result(c.columns, rows)
    else:
        print('totally cost',time_end-time_start)
