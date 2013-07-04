#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table, Literal
from sql.operators import And, Not, Less, Equal, NotEqual


class TestOperators(unittest.TestCase):
    table = Table('t')

    def test_and(self):
        and_ = And((self.table.c1, self.table.c2))
        self.assertEqual(str(and_), '("c1" AND "c2")')
        self.assertEqual(and_.params, ())

        and_ = And((Literal(True), self.table.c2))
        self.assertEqual(str(and_), '(%s AND "c2")')
        self.assertEqual(and_.params, (True,))

    def test_not(self):
        not_ = Not(self.table.c)
        self.assertEqual(str(not_), '(NOT "c")')
        self.assertEqual(not_.params, ())

        not_ = Not(Literal(False))
        self.assertEqual(str(not_), '(NOT %s)')
        self.assertEqual(not_.params, (False,))

    def test_less(self):
        less = Less(self.table.c1, self.table.c2)
        self.assertEqual(str(less), '("c1" < "c2")')
        self.assertEqual(less.params, ())

        less = Less(Literal(0), self.table.c2)
        self.assertEqual(str(less), '(%s < "c2")')
        self.assertEqual(less.params, (0,))

    def test_equal(self):
        equal = Equal(self.table.c1, self.table.c2)
        self.assertEqual(str(equal), '("c1" = "c2")')
        self.assertEqual(equal.params, ())

        equal = Equal(Literal('foo'), Literal('bar'))
        self.assertEqual(str(equal), '(%s = %s)')
        self.assertEqual(equal.params, ('foo', 'bar'))

        equal = Equal(self.table.c1, None)
        self.assertEqual(str(equal), '("c1" IS NULL)')
        self.assertEqual(equal.params, ())

        equal = Equal(Literal('test'), None)
        self.assertEqual(str(equal), '(%s IS NULL)')
        self.assertEqual(equal.params, ('test',))

        equal = Equal(None, self.table.c1)
        self.assertEqual(str(equal), '("c1" IS NULL)')
        self.assertEqual(equal.params, ())

        equal = Equal(None, Literal('test'))
        self.assertEqual(str(equal), '(%s IS NULL)')
        self.assertEqual(equal.params, ('test',))

    def test_not_equal(self):
        equal = NotEqual(self.table.c1, self.table.c2)
        self.assertEqual(str(equal), '("c1" != "c2")')
        self.assertEqual(equal.params, ())

        equal = NotEqual(self.table.c1, None)
        self.assertEqual(str(equal), '("c1" IS NOT NULL)')
        self.assertEqual(equal.params, ())

        equal = NotEqual(None, self.table.c1)
        self.assertEqual(str(equal), '("c1" IS NOT NULL)')
        self.assertEqual(equal.params, ())
