#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#
# this script runs and compares 22 tpc-h queries in both normal cursor mode and parallel cursor mode

import os
import importlib
import optparse

# unable to import GP2GP-CLIENT because of the '-' char
client_initializer = importlib.import_module("GP2GP-CLIENT.gp2gp.client_initializer")

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
                      
    parser.add_option('-l', '--level', type="string",
                      dest="log_level", help="log level: info|debug", default="info")

    return parser

def main():
    parser = create_options()
    options, _ = parser.parse_args()
    options.deploying = True
    c = client_initializer.initialize_client(options, test=True)
    # TODO: run and test the queries

