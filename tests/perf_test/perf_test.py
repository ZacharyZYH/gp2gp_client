#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#
# this script runs and compares 22 tpc-h queries in both normal cursor mode and parallel cursor mode

import logging
import optparse
import os

from gp2gp.client import GP2GPClient
from gp2gp.client_initializer import initialize_client


def create_options():
    usage = "usage: %prog [options]"
    description = '''GP2GP Performance Test Client'''

    parser = optparse.OptionParser(
        description=description, prog='client', usage=usage)

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

    parser.add_option('-c', '--client_conf', type="string",
                      dest="client_conf", help="the config file that stores the client machines", default="clients.conf")

    parser.add_option('-l', '--level', type="string",
                      dest="log_level", help="log level: info|debug", default="info")

    return parser

def main():
    parser = create_options()
    options, _ = parser.parse_args()
    options.perf_test = True
    options.deploying = False
    options.query = ""

    if options.log_level == 'debug':
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    # read the queries from data dir
    if os.path.basename(os.getcwd()) != "gp2gp_client":
        raise Exception("You should run the script in the gp2gp_client directory, but not" + os.path.basename(os.getcwd()))

    path = "tests/perf_test/data/"
    file_list = os.listdir(path)
    
    result = {}

    for filename in file_list:
        options.filename = path+filename
        options.is_normal = True
        logging.info("Running SQL query %s" % filename)
        cost1 = initialize_client(options)
        logging.info("normal cursor time: %f seconds" % cost1)
        options.is_normal = False
        cost2 = initialize_client(options)
        logging.info("parallel cursor time: %f seconds" % cost2)
        result[filename] = (cost1, cost2)

    print(result)
