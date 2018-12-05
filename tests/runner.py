#!/usr/bin/env python
# _*_ coding: utf-8 _*_
#

import os
import sys
import unittest

cur_dir = os.path.dirname(__file__)
sys.path.insert(0, os.path.join(cur_dir, '../'))

case_path = os.path.join(cur_dir, "../tests")


def search_test_case():
    discover = unittest.defaultTestLoader.discover(case_path,
                                                   pattern="test_*.py",
                                                   top_level_dir=None)
    return discover


if __name__ == "__main__":
    runner = unittest.TextTestRunner()
    runner.run(search_test_case())
