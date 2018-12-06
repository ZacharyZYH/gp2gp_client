#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import threading

from threading import Thread
from time import sleep

import psycopg2


def async_call(fn):
    def wrapper(*args, **kwargs):
        Thread(target=fn, args=args, kwargs=kwargs).start()

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


class GP2GPClient:

    def __init__(self, database, user, password, queries, host="127.0.0.1", port=5432):
        self.database = database
        self.user = user
        self.password = password
        self.queries = queries
        self.host = host
        self.port = port

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
        self.result = []

    def init(self):
        for cursor_name, sql in self.queries.items():
            sql = "declare %s parallel cursor for %s;" % (cursor_name, sql)
            self.init_cursor.execute(sql)

    @async_call
    def prepare(self, cursor_name=None):
        cursor_name = self.queries.keys()[0] if cursor_name is None else cursor_name
        self.init_cursor.execute("execute parallel cursor %s;" % cursor_name)

    def get_endpoints(self, cursor_name=None):
        endpoints = {}

        if cursor_name:
            self.status_cursor.execute("select * from gp_endpoints where cursorname='%s';" % cursor_name)
        else:
            self.status_cursor.execute("select * from gp_endpoints;")
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
                "status": row[5]
            }
            endpoints[endpoint["cursor_name"]] = endpoints.get(endpoint["cursor_name"], [])
            endpoints[endpoint["cursor_name"]].append(endpoint)

        return endpoints

    def wait_for_ready(self, cursor_name=None):

        while True:

            is_all_ready = True
            endpoints = self.get_endpoints(cursor_name)

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
        count = AtomicInteger()
        for cursor_name, endpoints in self.endpoints.items():
            for endpoint in endpoints:
                count.inc()
                self.fetch_one(endpoint, count)

        while count.value != 0:
            print "There are %d threads left.\n" % count.value
            sleep(0.1)

    @async_call
    def fetch_one(self, endpoint, count):
        try:
            conn = psycopg2.connect(
                database=self.database,
                # user=self.user,
                user="gpadmin",
                # password=endpoint.get('token'),
                password="123456",
                host=endpoint.get('hostname'),
                port=endpoint.get('port'),
                options="-c gp_session_role=retrieve"
            )

            self.data_conns.append(conn)
            cursor = conn.cursor()
            cursor.execute('retrieve "%s"' % endpoint.get('token'))
            rows = cursor.fetchall()
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
        if len(self.queries) != 1:
            raise Exception("the length of queries should be equal to !")

        self.init()
        self.prepare()
        self.wait_for_ready()
        self.fetch_all()
        self.close()

        return self.result
