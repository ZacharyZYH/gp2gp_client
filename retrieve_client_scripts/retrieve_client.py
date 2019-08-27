#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#
import logging
import optparse
import os
import threading
from time import sleep

import psycopg2


def async_call(fn):
    def wrapper(*args, **kwargs):
        threading.Thread(target=fn, args=args, kwargs=kwargs).start()

    return wrapper


class AtomicInteger():
    def __init__(self, value=0):
        self._value = value
        self._lock = threading.Lock()

    def inc(self):
        with self._lock:
            self._value += 1
            return self._value

    def dec(self):
        with self._lock:
            self._value -= 1
            return self._value

    @property
    def value(self):
        with self._lock:
            return self._value

    @value.setter
    def value(self, v):
        with self._lock:
            self._value = v
            return self._value


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

    parser.add_option('-T', '--test', action="store_true",
                      dest="perf_test", help="execute for performance testing(not generating result)", default=False)

    return parser

@async_call
def retrieve_one(options, port, count):
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
        cursor.execute('retrieve all from "%s"' % options.token)
        rows = cursor.fetchall()
        if not options.perf_test:
            print(rows)

    except Exception as e:
        print e
    finally:
        count.dec()

if __name__ == '__main__':
    parser = create_options()
    options, args = parser.parse_args()

    if options.log_level == 'debug':
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # parse the port string into a list
    ports = options.ports.split(",")

    count = AtomicInteger()

    for port in ports:
        count.inc()
        retrieve_one(options, port, count)

    while count.value != 0:
        logging.debug("There are %d threads left.\n" % count.value)
        sleep(0.1)
