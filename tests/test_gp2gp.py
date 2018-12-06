#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import os
import unittest
import psycopg2

from gp2gp.client import GP2GPClient

cur_dir = os.path.dirname(__file__)


class TestGp2Gp(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        print "Init Test Suite"

    @classmethod
    def tearDownClass(cls):
        print "Destroy Test Suite"

    def printEnv(self):
        print "\nDATABASE:%s" % self.database
        print "USER: %s" % self.user
        print "PASS: %s" % self.password
        print "HOST: %s" % self.host
        print "PORT: %s" % self.port

    def run_sql(self, sql, cursor=None):

        if cursor is None:
            cursor = self.runner

        ret = []

        cursor.execute(sql)
        rows = cursor.fetchall()

        for row in rows:
            ret.append(row)

        return ret

    def setUp(self):
        os.system("psql -d postgres -f " + os.path.join(cur_dir, "data/db.sql"))

        self.database = os.environ.get("GP2GP_DB", "testdb")
        self.user = os.environ.get("GP2GP_USER", "baotingfang")
        self.password = ""
        self.host = os.environ.get("GP2GP_HOST", "127.0.0.1")
        self.port = os.environ.get("GP2GP_PORT", 5432)

        self.runner = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        ).cursor()

        self.client = GP2GPClient(
            database=self.database,
            user=self.user,
            password=self.password,
            queries=[],
            host=self.host,
            port=self.port
        )

    def tearDown(self):
        self.runner.connection.commit()
        self.runner.connection.close()
        os.system("dropdb testdb")

    def testGp2Gp_case1(self):
        queries = {
            "c1": "select * from t1",
        }
        self.client.queries = queries
        result = self.client.get_data()
        self.assertEqual(len(result), 1024, u"应该是1024")

    def testGp2Gp_case2(self):
        queries = {
            "c1": "select * from t1",
        }

        self.client.queries = queries
        self.client.init()

        # create a parallel cursor, we can find it in pg_cursors,
        # its is_parallel is true
        ret = self.run_sql("select name, is_parallel from pg_cursors where name='c1';", self.client.init_cursor)
        self.assertEqual(ret[0][0], 'c1')
        self.assertTrue(ret[0][1])

        ret = self.client.get_endpoints("c1")

        self.client.prepare()
        self.client.wait_for_ready()

        # when run
        self.assertTrue(self.client.endpoints["c1"][0]["status"] == "READY", "")

        self.client.fetch_all()
        self.client.close()
