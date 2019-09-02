#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import logging
import os
import subprocess
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

def get_clients(client_conf):
    file = open(client_conf)
    clients = file.readlines()
    file.close()
    if not clients:
        raise Exception("no valid hosts in the client config file")
    return clients

class GP2GPClient:

    def __init__(self, database, user, password, queries, client_conf, host="127.0.0.1", port=5432, is_normal=False, perf_test=False):
        self.database = database
        self.user = user
        self.password = password
        self.queries = queries
        self.host = host
        self.port = port
        self.is_normal = is_normal
        self.perf_test = perf_test
        self.fetch_size = "1000"
        if not is_normal:
            self.client_hosts = get_clients(client_conf)

        self.init_conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

        self.status_conn = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )

        self.init_cursor = self.init_conn.cursor()
        self.status_cursor = self.status_conn.cursor()

        self.data_conns = []
        self.endpoints = {}
        self.columns = []
        self.result = []

    def get_hosts(self):
        sql = "SELECT DISTINCT hostname FROM gp_segment_configuration WHERE role = 'p'"
        self.init_cursor.execute(sql)
        return self.init_cursor.fetchall()

    def get_segments(self):
        sql = "SELECT DISTINCT hostname FROM gp_segment_configuration WHERE role = 'p' AND content>-1"
        self.init_cursor.execute(sql)
        return self.init_cursor.fetchall()

    def init(self):
        for cursor_name, sql in self.queries.items():
            if self.is_normal:
                sql = "declare %s cursor for %s;" % (cursor_name, sql)
            else:
                sql = "declare %s parallel cursor for %s;" % (cursor_name, sql)
            logging.debug("Init: %s", sql)
            self.init_cursor.execute(sql)

    def get_token(self):
        self.init_cursor.execute("select * from gp_endpoints;")
        row = self.init_cursor.fetchone()
        return row[0]

    @async_call
    def prepare(self, cursor_name=None):
        cursor_name = self.queries.keys()[0] if cursor_name is None else cursor_name
        sql = "execute parallel cursor %s;" % cursor_name
        logging.debug("Prepare: %s", sql)
        self.init_cursor.execute(sql)

    def get_endpoints(self, token=None):
        endpoints = {}

        if token:
            self.status_cursor.execute("select * from gp_endpoints_info(true) where token='%s';" % token)
        else:
            self.status_cursor.execute("select * from gp_endpoints_info(true);")
        try:
            rows = self.status_cursor.fetchall()
        except:
            return {}

        for row in rows:
            endpoint = {
                "token": row[0],
                "cursor_name": row[1],
                "session_id": row[2],
                "hostname": row[3],
                "port": row[4],
                "db_id": row[5],
                "user_id": row[6],
                "status": row[7]
            }
            endpoints[endpoint["hostname"]] = endpoints.get(endpoint["hostname"], [])
            endpoints[endpoint["hostname"]].append(endpoint)

        logging.debug("Endpoints: %s", endpoints)

        return endpoints

    def wait_for_ready(self, token=None):

        while True:

            is_all_ready = True
            endpoints = self.get_endpoints(token)

            for _, values in endpoints.items():
                for ep in values:
                    if ep.get("status") != 'READY':
                        is_all_ready = False
                        break

            if is_all_ready:
                break
            else:
                sleep(1)

        self.endpoints = endpoints

    def fetch_all(self):
        self.result = []
        count = AtomicInteger()
        client_index = 0
        for _, endpoints_per_machine in self.endpoints.iteritems():
            count.inc()
            self.fetch_one(endpoints_per_machine, count, self.client_hosts[client_index])
            if client_index == len(self.client_hosts):
                client_index = 0
            else:
                client_index += 1

        while count.value != 0:
            logging.debug("There are %d threads left.\n" % count.value)
            sleep(0.1)

    # retrieve all segments of a same machine
    @async_call
    def fetch_one(self, endpoints, count, client_host):
        logging.debug("fetch_one: fetching segments on seg_host %s from client host %s", endpoints[0]["hostname"], client_host)

        try:
            # join all ports to one string
            ports_arr = []
            for endpoint in endpoints:
                ports_arr.append(str(endpoint["port"]))
            ports = ",".join(ports_arr)
            logging.debug("the ports are joined into one string: %s", ports)

            user = self.user or "gpadmin"
            cmd_arg = [ "ssh", user + "@" + client_host, 
                        "python", "retrieve_client_scripts/retrieve_client.py", 
                        "-d", self.database, 
                        "-H", endpoints[0]["hostname"], 
                        "-p", ports, 
                        # "-u", self.user or "",
                        "-t", endpoints[0]["token"],
                        "-s", self.fetch_size
                    ]
            if self.perf_test:
                cmd_arg.append("-T")
                subprocess.check_call(cmd_arg)
            else:
                rows = subprocess.check_output(cmd_arg)

                # if count.value == 1:
                # TODO add title

                self.result.extend(rows)
        except Exception as e:
            print e
            # os._exit(-1)
        finally:
            count.dec()

    def close(self):
        if self.init_conn and (not self.init_conn.closed):
            self.init_conn.commit()
            self.init_conn.close()

        if self.status_conn and (not self.status_conn.closed):
            self.status_conn.commit()
            self.status_conn.close()

        for db_conn in self.data_conns:
            if not db_conn.closed:
                db_conn.commit()
                db_conn.close()

    def get_data(self):
        if(self.is_normal):
            self.init()
            retrieve_sql = "fetch " + self.fetch_size + " from %s;" % self.queries.keys()[0]
            while True:
                self.init_cursor.execute(retrieve_sql)
                rows = self.init_cursor.fetchall()
                if not rows:
                    break
                if not self.perf_test:
                    self.result.extend(rows)
            if not self.perf_test:
                self.columns = [desc[0] for desc in self.init_cursor.description]
            self.init_cursor.execute("close %s;" % self.queries.keys()[0])
        else:
            if len(self.queries) != 1:
                raise Exception("the length of queries should be equal to 1")

            self.init()
            token = self.get_token()
            self.endpoints = self.get_endpoints(token)
            self.prepare()
            self.wait_for_ready(token)
            self.fetch_all()
            self.close()

        return self.result
