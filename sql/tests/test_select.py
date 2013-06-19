#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table, Join


class TestSelect(unittest.TestCase):
    table = Table('t')

    def test_select1(self):
        query = self.table.select()
        self.assertEqual(str(query), 'SELECT * FROM "t" AS "a"')
        self.assertEqual(query.params, ())

    def test_select2(self):
        query = self.table.select(self.table.c)
        self.assertEqual(str(query), 'SELECT "a"."c" FROM "t" AS "a"')
        self.assertEqual(query.params, ())

        query.columns += (self.table.c2,)
        self.assertEqual(str(query),
            'SELECT "a"."c", "a"."c2" FROM "t" AS "a"')

    def test_select3(self):
        query = self.table.select(where=(self.table.c == 'foo'))
        self.assertEqual(str(query),
            'SELECT * FROM "t" AS "a" WHERE ("a"."c" = %s)')
        self.assertEqual(query.params, ('foo',))

    def test_select_union(self):
        query1 = self.table.select()
        query2 = Table('t2').select()
        union = query1 | query2
        self.assertEqual(str(union),
            'SELECT * FROM "t" AS "a" UNION SELECT * FROM "t2" AS "b"')
        union.all_ = True
        self.assertEqual(str(union),
            'SELECT * FROM "t" AS "a" UNION ALL '
            'SELECT * FROM "t2" AS "b"')
        self.assertEqual(str(union.select()),
            'SELECT * FROM ('
            'SELECT * FROM "t" AS "b" UNION ALL '
            'SELECT * FROM "t2" AS "c") AS "a"')
        query1.where = self.table.c == 'foo'
        self.assertEqual(str(union),
            'SELECT * FROM "t" AS "a" WHERE ("a"."c" = %s) UNION ALL '
            'SELECT * FROM "t2" AS "b"')
        self.assertEqual(union.params, ('foo',))

    def test_select_join(self):
        t1 = Table('t1')
        t2 = Table('t2')
        join = Join(t1, t2)

        self.assertEqual(str(join.select()),
            'SELECT * FROM "t1" AS "a" INNER JOIN "t2" AS "b"')
        self.assertEqual(str(join.select(getattr(t1, '*'))),
            'SELECT "a".* FROM "t1" AS "a" INNER JOIN "t2" AS "b"')
