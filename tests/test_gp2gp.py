#!/usr/bin/env python
# _*_ coding: utf-8 _*_

import os
import unittest
from time import sleep

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
        try:
            rows = cursor.fetchall()
        except:
            return []

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

    # a simple test case to verify
    # the number of the table
    def testGp2Gp_case1(self):
        queries = {
            "c1": "select * from t1",
        }
        self.client.queries = queries
        result = self.client.get_data()
        self.assertEqual(len(result), 1024, "the size of the result should be 1024")

    def testGp2Gp_case2(self):
        queries = {
            "c1": "select * from t1",
        }

        self.client.queries = queries
        self.client.init()

        # Declare a parallel cursor, we can find it in pg_cursors,
        # its is_parallel is true
        ret = self.run_sql("select name, is_parallel from pg_cursors where name='c1';", self.client.init_cursor)
        self.assertEqual(ret[0][0], 'c1')
        self.assertTrue(ret[0][1])

        # After declare a parallel cursor, we can find tokens in gp_endpoints,
        # and all status should be INIT
        ret = self.client.get_endpoints("c1")
        for item in ret["c1"]:
            self.assertTrue(item["status"], "INIT")

        self.client.prepare()
        self.client.wait_for_ready()

        # when execute the parallel cursor,
        # token status should be READY
        ret = self.client.get_endpoints("c1")
        for item in ret["c1"]:
            self.assertTrue(item["status"], "READY")

        self.client.fetch_all()
        # verify the result
        self.assertEqual(len(self.client.result), 1024)

        # After retrieve data from all endpoints,
        # the status should become INIT
        ret = self.client.get_endpoints("c1")
        for item in ret["c1"]:
            self.assertTrue(item["status"], "INIT")

        # close parallel cursor c1
        self.run_sql("close c1", self.client.init_cursor)
        # no token related to this cursor
        ret = self.client.get_endpoints("c1")
        self.assertEqual(len(ret), 0)

        # not exist in pg_cursors any more
        ret = self.run_sql("select name, is_parallel from pg_cursors where name='c1';", self.client.init_cursor)
        self.assertEqual(len(ret), 0)

        self.client.close()

    def testGp2Gp_case3(self):
        queries = {
            "c1": "select count(*) from t1",
        }

        self.client.queries = queries
        self.client.init()

        # After declare a parallel cursor, we can find tokens in gp_endpoints,
        # and all status should be INIT
        ret = self.client.get_endpoints("c1")
        for item in ret["c1"]:
            self.assertTrue(item["status"], "INIT")

        # endpoint on master
        self.assertEqual(len(ret), 1)
        self.client.prepare()
        self.client.wait_for_ready()
        self.client.fetch_all()
        self.assertEqual(len(self.client.result), 1)

        # the result should be the tuple count of this table.
        self.assertEqual(self.client.result[0][0], 1024)

        self.client.close()


    def testGp2Gp_case4(self):
        # In this case, we declare some parallel cursors
        # in one session and execute them one by one.
        queries = {
            "c0": "select count(*) from t1",
            "c1": "select * from t1 where c1 <10 order by c1",
            "c2": "select * from t2 order by c1",
            "c3": "select count(*) from t2 where c1 < 20",
            "c4": "select * from t1,t3 where t1.c1 = t3.c1 order by t1.c1",
            "c5": "select count(*) from t1,t2 where t1.c1 = t2.c1",
            "c6": "select sum(c1) from t3",
            "c7": "select * from t1,t2,t3 where t1.c1=t2.c1 and t2.c1 =t3.c1 order by t1.c1"
        }

        # declare parallel cursors
        self.client.queries = queries
        self.client.init()

        for i in range(len(queries)):
            cursor = 'c%d' % i
            cmd = "select name, is_parallel from pg_cursors where name='%s'" % cursor
            ret = self.run_sql(cmd, self.client.init_cursor)
            self.assertTrue(ret[0][1])

            ret = self.client.get_endpoints(cursor)
            for item in ret[cursor]:
                self.assertTrue(item["status"], "INIT")

            # execute cursor and verify the result.
            self.client.prepare(cursor)
            self.client.wait_for_ready(cursor)
            self.client.fetch_all()
            # execute the query without parallel cursor
            # the results should be same
            ret = self.run_sql(queries[cursor])
            self.assertEqual(len(ret), len(self.client.result))
            for i in range(len(ret)):
                self.assertEqual(self.client.result[i], ret[i])

        self.client.close()
        