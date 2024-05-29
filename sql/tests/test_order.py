# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.
import unittest

from sql import (
    Asc, Column, Desc, Flavor, Literal, NullOrder, NullsFirst, NullsLast,
    Order, Table)


class TestOrder(unittest.TestCase):
    column = Column(Table('t'), 'c')

    def test_asc(self):
        self.assertEqual(str(self.column.asc), '"c" ASC')

    def test_desc(self):
        self.assertEqual(str(self.column.desc), '"c" DESC')

    def test_nulls_first(self):
        self.assertEqual(str(self.column.nulls_first), '"c" NULLS FIRST')
        self.assertEqual(str(Asc(self.column).nulls_first),
            '"c" ASC NULLS FIRST')

    def test_nulls_last(self):
        self.assertEqual(str(self.column.nulls_last), '"c" NULLS LAST')
        self.assertEqual(str(Asc(self.column).nulls_last),
            '"c" ASC NULLS LAST')

    def test_null_order_case_values(self):
        with self.assertRaises(NotImplementedError):
            NullOrder(self.column)._case_values()

    def test_no_null_ordering(self):
        try:
            Flavor.set(Flavor(null_ordering=False))

            exp = NullsFirst(self.column)
            self.assertEqual(str(exp),
                'CASE WHEN ("c" IS NULL) THEN %s ELSE %s END ASC, "c"')
            self.assertEqual(exp.params, (0, 1))

            exp = NullsFirst(Desc(self.column))
            self.assertEqual(str(exp),
                'CASE WHEN ("c" IS NULL) THEN %s ELSE %s END ASC, "c" DESC')
            self.assertEqual(exp.params, (0, 1))

            exp = NullsLast(Literal(2))
            self.assertEqual(str(exp),
                'CASE WHEN (%s IS NULL) THEN %s ELSE %s END ASC, %s')
            self.assertEqual(exp.params, (2, 1, 0, 2))
        finally:
            Flavor.set(Flavor())

    def test_order_query(self):
        table = Table('t')
        column = Column(table, 'c')
        query = table.select(column)
        self.assertEqual(str(Asc(query)),
            '(SELECT "a"."c" FROM "t" AS "a") ASC')
        self.assertEqual(str(Desc(query)),
            '(SELECT "a"."c" FROM "t" AS "a") DESC')

    def test_invalid_expression(self):
        with self.assertRaises(ValueError):
            Order('foo')
