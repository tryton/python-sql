#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table
from sql.operators import And, Not, Less, Equal, NotEqual


class TestOperators(unittest.TestCase):
    table = Table('t')

    def test_and(self):
        and_ = And((self.table.c1, self.table.c2))
        self.assertEqual(str(and_), '("c1" AND "c2")')

    def test_not(self):
        not_ = Not(self.table.c)
        self.assertEqual(str(not_), '(NOT "c")')

    def test_less(self):
        less = Less(self.table.c1, self.table.c2)
        self.assertEqual(str(less), '("c1" < "c2")')

    def test_equal(self):
        equal = Equal(self.table.c1, self.table.c2)
        self.assertEqual(str(equal), '("c1" = "c2")')

        equal = Equal(self.table.c1, None)
        self.assertEqual(str(equal), '("c1" IS NULL)')

        equal = Equal(None, self.table.c1)
        self.assertEqual(str(equal), '("c1" IS NULL)')

    def test_not_equal(self):
        equal = NotEqual(self.table.c1, self.table.c2)
        self.assertEqual(str(equal), '("c1" != "c2")')

        equal = NotEqual(self.table.c1, None)
        self.assertEqual(str(equal), '("c1" IS NOT NULL)')

        equal = NotEqual(None, self.table.c1)
        self.assertEqual(str(equal), '("c1" IS NOT NULL)')
