#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table
from sql.functions import Abs


class TestFunctions(unittest.TestCase):
    table = Table('t')

    def test_abs(self):
        abs_ = Abs(self.table.c1)
        self.assertEqual(str(abs_), 'ABS("c1")')
        self.assertEqual(abs_.params, ())

        abs_ = Abs(-12)
        self.assertEqual(str(abs_), 'ABS(-12)')
        self.assertEqual(abs_.params, ())
