# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.
import unittest

from sql import AliasManager, Join, Table
from sql.functions import Now


class TestJoin(unittest.TestCase):
    def test_join(self):
        t1 = Table('t1')
        t2 = Table('t2')
        join = Join(t1, t2)
        with AliasManager():
            self.assertEqual(str(join), '"t1" AS "a" INNER JOIN "t2" AS "b"')
            self.assertEqual(tuple(join.params), ())

        join.condition = t1.c == t2.c
        with AliasManager():
            self.assertEqual(str(join),
                '"t1" AS "a" INNER JOIN "t2" AS "b" ON ("a"."c" = "b"."c")')

    def test_join_invalid_left(self):
        with self.assertRaises(ValueError):
            Join('foo', Table('t1'))

    def test_join_invalid_right(self):
        with self.assertRaises(ValueError):
            Join(Table('t1'), 'foo')

    def test_join_invalid_condition(self):
        with self.assertRaises(ValueError):
            Join(Table('t1'), Table('t2'), condition='foo')

    def test_join_invalid_type(self):
        with self.assertRaises(ValueError):
            Join(Table('t1'), Table('t2'), type_='foo')

    def test_join_subselect(self):
        t1 = Table('t1')
        t2 = Table('t2')
        select = t2.select()
        join = Join(t1, select)
        join.condition = t1.c == select.c
        with AliasManager():
            self.assertEqual(str(join),
                '"t1" AS "a" INNER JOIN (SELECT * FROM "t2" AS "c") AS "b" '
                'ON ("a"."c" = "b"."c")')
            self.assertEqual(tuple(join.params), ())

    def test_join_function(self):
        t1 = Table('t1')
        join = Join(t1, Now())
        with AliasManager():
            self.assertEqual(str(join), '"t1" AS "a" INNER JOIN NOW() AS "b"')
            self.assertEqual(tuple(join.params), ())

    def test_join_methods(self):
        t1 = Table('t1')
        t2 = Table('t2')
        for method in [
                'left_join', 'left_outer_join',
                'right_join', 'right_outer_join',
                'full_join', 'full_outer_join', 'cross_join']:
            with self.subTest(method=method):
                join = getattr(t1, method)(t2)
                type_ = method[:-len('_join')].replace('_', ' ').upper()
                self.assertEqual(join.type_, type_)

    def test_join_alias(self):
        join = Join(Table('t1'), Table('t2'))
        with self.assertRaises(AttributeError):
            join.alias
        with self.assertRaises(AttributeError):
            join.has_alias
