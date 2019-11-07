# -*- coding: utf-8 -*-
#
# Copyright (c) 2011-2016, Cédric Krier
# Copyright (c) 2013-2014, Nicolas Évrard
# Copyright (c) 2011-2016, B2CK
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import unittest
import warnings
from copy import deepcopy

from sql import Table, Join, Union, Literal, Flavor, For, With, Window, Select
from sql.functions import Now, Function, Rank, DatePart
from sql.aggregate import Min


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

    def test_select_without_from(self):
        query = Select([Literal(1)])
        self.assertEqual(str(query), 'SELECT %s')
        self.assertEqual(query.params, (1,))

    def test_select_select(self):
        query = Select([Select([Literal(1)])])
        self.assertEqual(str(query), 'SELECT (SELECT %s)')
        self.assertEqual(query.params, (1,))

    def test_select_select_as(self):
        query = Select([Select([Literal(1)]).as_('foo')])
        self.assertEqual(str(query), 'SELECT (SELECT %s) AS "foo"')
        self.assertEqual(query.params, (1,))

    def test_select_distinct(self):
        query = self.table.select(self.table.c, distinct=True)
        self.assertEqual(
            str(query), 'SELECT DISTINCT "a"."c" FROM "t" AS "a"')
        self.assertEqual(query.params, ())

    def test_select_distinct_on(self):
        query = self.table.select(self.table.c, distinct_on=self.table.c)
        self.assertEqual(
            str(query), 'SELECT DISTINCT ON ("a"."c") "a"."c" FROM "t" AS "a"')
        self.assertEqual(query.params, ())

        query = self.table.select(
            self.table.c, distinct_on=[self.table.a, self.table.b])
        self.assertEqual(
            str(query),
            'SELECT DISTINCT ON ("a"."a", "a"."b") "a"."c" FROM "t" AS "a"')
        self.assertEqual(query.params, ())

    def test_select_from_list(self):
        t2 = Table('t2')
        t3 = Table('t3')
        query = (self.table + t2 + t3).select(self.table.c, getattr(t2, '*'))
        self.assertEqual(str(query),
            'SELECT "a"."c", "b".* FROM "t" AS "a", "t2" AS "b", "t3" AS "c"')
        self.assertEqual(query.params, ())

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

        union = Union(query1)
        self.assertEqual(str(union), str(query1))
        self.assertEqual(union.params, query1.params)

    def test_select_union_order(self):
        query1 = self.table.select()
        query2 = Table('t2').select()
        union = query1 | query2
        union.order_by = Literal(1)
        self.assertEqual(str(union),
            'SELECT * FROM "t" AS "a" UNION '
            'SELECT * FROM "t2" AS "b" '
            'ORDER BY %s')
        self.assertEqual(union.params, (1,))

    def test_select_intersect(self):
        query1 = self.table.select()
        query2 = Table('t2').select()
        intersect = query1 & query2
        self.assertEqual(str(intersect),
            'SELECT * FROM "t" AS "a" INTERSECT SELECT * FROM "t2" AS "b"')

        from sql import Intersect, Interesect
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter('always')
            interesect = Interesect(query1, query2)
            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[-1].category, DeprecationWarning))
            if hasattr(self, 'assertIn'):
                self.assertIn('Interesect query is deprecated, use Intersect',
                    str(w[-1].message))
        self.assertTrue(isinstance(interesect, Intersect))

    def test_select_except(self):
        query1 = self.table.select()
        query2 = Table('t2').select()
        except_ = query1 - query2
        self.assertEqual(str(except_),
            'SELECT * FROM "t" AS "a" EXCEPT SELECT * FROM "t2" AS "b"')

    def test_select_join(self):
        t1 = Table('t1')
        t2 = Table('t2')
        join = Join(t1, t2)

        self.assertEqual(str(join.select()),
            'SELECT * FROM "t1" AS "a" INNER JOIN "t2" AS "b"')
        self.assertEqual(str(join.select(getattr(t1, '*'))),
            'SELECT "a".* FROM "t1" AS "a" INNER JOIN "t2" AS "b"')

    def test_select_subselect(self):
        t1 = Table('t1')
        select = t1.select()
        self.assertEqual(str(select.select()),
            'SELECT * FROM (SELECT * FROM "t1" AS "b") AS "a"')
        self.assertEqual(select.params, ())

    def test_select_function(self):
        query = Now().select()
        self.assertEqual(str(query), 'SELECT * FROM NOW() AS "a"')
        self.assertEqual(query.params, ())

    def test_select_function_columns_definitions(self):
        class Crosstab(Function):
            _function = 'CROSSTAB'

        query = Crosstab('query1', 'query2',
            columns_definitions=[
                ('c1', 'INT'), ('c2', 'CHAR'), ('c3', 'BOOL')]).select()
        self.assertEqual(str(query), 'SELECT * FROM CROSSTAB(%s, %s) '
            'AS "a" ("c1" INT, "c2" CHAR, "c3" BOOL)')
        self.assertEqual(query.params, ('query1', 'query2'))

    def test_select_group_by(self):
        column = self.table.c
        query = self.table.select(column, group_by=column)
        self.assertEqual(str(query),
            'SELECT "a"."c" FROM "t" AS "a" GROUP BY "a"."c"')
        self.assertEqual(query.params, ())

        output = column.as_('c1')
        query = self.table.select(output, group_by=output)
        self.assertEqual(str(query),
            'SELECT "a"."c" AS "c1" FROM "t" AS "a" GROUP BY "c1"')
        self.assertEqual(query.params, ())

        query = self.table.select(Literal('foo'), group_by=Literal('foo'))
        self.assertEqual(str(query),
            'SELECT %s FROM "t" AS "a" GROUP BY %s')
        self.assertEqual(query.params, ('foo', 'foo'))

    def test_select_having(self):
        col1 = self.table.col1
        col2 = self.table.col2
        query = self.table.select(col1, Min(col2),
            having=(Min(col2) > 3))
        self.assertEqual(str(query),
            'SELECT "a"."col1", MIN("a"."col2") FROM "t" AS "a" '
            'HAVING (MIN("a"."col2") > %s)')
        self.assertEqual(query.params, (3,))

    def test_select_order(self):
        c = self.table.c
        query = self.table.select(c, order_by=Literal(1))
        self.assertEqual(str(query),
            'SELECT "a"."c" FROM "t" AS "a" ORDER BY %s')
        self.assertEqual(query.params, (1,))

    def test_select_limit_offset(self):
        try:
            Flavor.set(Flavor(limitstyle='limit'))
            query = self.table.select(limit=50, offset=10)
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a" LIMIT 50 OFFSET 10')
            self.assertEqual(query.params, ())

            query.limit = None
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a" OFFSET 10')
            self.assertEqual(query.params, ())

            query.offset = 0
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a"')
            self.assertEqual(query.params, ())

            Flavor.set(Flavor(limitstyle='limit', max_limit=-1))

            query.offset = None
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a"')
            self.assertEqual(query.params, ())

            query.offset = 0
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a"')
            self.assertEqual(query.params, ())

            query.offset = 10
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a" LIMIT -1 OFFSET 10')
            self.assertEqual(query.params, ())
        finally:
            Flavor.set(Flavor())

    def test_select_offset_fetch(self):
        try:
            Flavor.set(Flavor(limitstyle='fetch'))
            query = self.table.select(limit=50, offset=10)
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a" '
                'OFFSET (10) ROWS FETCH FIRST (50) ROWS ONLY')
            self.assertEqual(query.params, ())

            query.limit = None
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a" OFFSET (10) ROWS')
            self.assertEqual(query.params, ())

            query.offset = 0
            self.assertEqual(str(query),
                'SELECT * FROM "t" AS "a"')
            self.assertEqual(query.params, ())
        finally:
            Flavor.set(Flavor())

    def test_select_rownum(self):
        try:
            Flavor.set(Flavor(limitstyle='rownum'))
            query = self.table.select(limit=50, offset=10)
            self.assertEqual(str(query),
                'SELECT "a".* FROM ('
                    'SELECT "b".*, ROWNUM AS "rnum" FROM ('
                        'SELECT * FROM "t" AS "c") AS "b" '
                    'WHERE (ROWNUM <= %s)) AS "a" '
                'WHERE ("rnum" > %s)')
            self.assertEqual(query.params, (60, 10))

            query = self.table.select(
                self.table.c1.as_('col1'), self.table.c2.as_('col2'),
                limit=50, offset=10)
            self.assertEqual(str(query),
                'SELECT "a"."col1", "a"."col2" FROM ('
                    'SELECT "b"."col1", "b"."col2", ROWNUM AS "rnum" FROM ('
                        'SELECT "c"."c1" AS "col1", "c"."c2" AS "col2" '
                        'FROM "t" AS "c") AS "b" '
                    'WHERE (ROWNUM <= %s)) AS "a" '
                'WHERE ("rnum" > %s)')
            self.assertEqual(query.params, (60, 10))

            subquery = query.select(query.col1, query.col2)
            self.assertEqual(str(subquery),
                'SELECT "a"."col1", "a"."col2" FROM ('
                    'SELECT "b"."col1", "b"."col2" FROM ('
                        'SELECT "a"."col1", "a"."col2", ROWNUM AS "rnum" '
                        'FROM ('
                            'SELECT "c"."c1" AS "col1", "c"."c2" AS "col2" '
                            'FROM "t" AS "c") AS "a" '
                        'WHERE (ROWNUM <= %s)) AS "b" '
                    'WHERE ("rnum" > %s)) AS "a"')
            # XXX alias of query is reused but not a problem
            # as it is hidden in subquery
            self.assertEqual(query.params, (60, 10))

            query = self.table.select(limit=50, offset=10,
                order_by=[self.table.c])
            self.assertEqual(str(query),
                'SELECT "a".* FROM ('
                    'SELECT "b".*, ROWNUM AS "rnum" FROM ('
                        'SELECT * FROM "t" AS "c" ORDER BY "c"."c") AS "b" '
                    'WHERE (ROWNUM <= %s)) AS "a" '
                'WHERE ("rnum" > %s)')
            self.assertEqual(query.params, (60, 10))

            query = self.table.select(limit=50)
            self.assertEqual(str(query),
                'SELECT "a".* FROM ('
                    'SELECT * FROM "t" AS "b") AS "a" '
                'WHERE (ROWNUM <= %s)')
            self.assertEqual(query.params, (50,))

            query = self.table.select(offset=10)
            self.assertEqual(str(query),
                'SELECT "a".* FROM ('
                    'SELECT "b".*, ROWNUM AS "rnum" FROM ('
                        'SELECT * FROM "t" AS "c") AS "b") AS "a" '
                'WHERE ("rnum" > %s)')
            self.assertEqual(query.params, (10,))

            query = self.table.select(self.table.c.as_('col'),
                where=self.table.c >= 20,
                limit=50, offset=10)
            self.assertEqual(str(query),
                'SELECT "a"."col" FROM ('
                    'SELECT "b"."col", ROWNUM AS "rnum" FROM ('
                        'SELECT "c"."c" AS "col" FROM "t" AS "c" '
                        'WHERE ("c"."c" >= %s)) AS "b" '
                    'WHERE (ROWNUM <= %s)) AS "a" '
                'WHERE ("rnum" > %s)')
            self.assertEqual(query.params, (20, 60, 10))
        finally:
            Flavor.set(Flavor())

    def test_select_for(self):
        c = self.table.c
        query = self.table.select(c, for_=For('UPDATE'))
        self.assertEqual(str(query),
            'SELECT "a"."c" FROM "t" AS "a" FOR UPDATE')
        self.assertEqual(query.params, ())

    def test_copy(self):
        query = self.table.select()
        copy_query = deepcopy(query)
        self.assertNotEqual(query, copy_query)
        self.assertEqual(str(copy_query), 'SELECT * FROM "t" AS "a"')
        self.assertEqual(copy_query.params, ())

    def test_with(self):
        w = With(query=self.table.select(self.table.c1))

        query = w.select(with_=[w])
        self.assertEqual(str(query),
            'WITH "a" AS (SELECT "b"."c1" FROM "t" AS "b") '
            'SELECT * FROM "a" AS "a"')
        self.assertEqual(query.params, ())

    def test_window(self):
        query = self.table.select(Min(self.table.c1,
                window=Window([self.table.c2])))

        self.assertEqual(str(query),
            'SELECT MIN("a"."c1") OVER "b" FROM "t" AS "a" '
            'WINDOW "b" AS (PARTITION BY "a"."c2")')
        self.assertEqual(query.params, ())

        query = self.table.select(Rank(window=Window([])))
        self.assertEqual(str(query),
            'SELECT RANK() OVER "b" FROM "t" AS "a" '
            'WINDOW "b" AS ()')
        self.assertEqual(query.params, ())

        window = Window([self.table.c1])
        query = self.table.select(
            Rank(filter_=self.table.c1 > 0, window=window),
            Min(self.table.c1, window=window))
        self.assertEqual(str(query),
            'SELECT RANK() FILTER (WHERE ("a"."c1" > %s)) OVER "b", '
            'MIN("a"."c1") OVER "b" FROM "t" AS "a" '
            'WINDOW "b" AS (PARTITION BY "a"."c1")')
        self.assertEqual(query.params, (0,))

        window = Window([DatePart('year', self.table.date_col)])
        query = self.table.select(
            Min(self.table.c1, window=window))
        self.assertEqual(str(query),
            'SELECT MIN("a"."c1") OVER "b" FROM "t" AS "a" '
            'WINDOW "b" AS (PARTITION BY DATE_PART(%s, "a"."date_col"))')
        self.assertEqual(query.params, ('year',))

    def test_order_params(self):
        with_ = With(query=self.table.select(self.table.c,
                where=(self.table.c > 1)))
        w = Window([Literal(8)])
        query = Select([Literal(2), Min(self.table.c, window=w)],
            from_=self.table.select(where=self.table.c > 3),
            with_=with_,
            where=self.table.c > 4,
            group_by=[Literal(5)],
            order_by=[Literal(6)],
            having=Literal(7))
        self.assertEqual(query.params, (1, 2, 3, 4, 5, 6, 7, 8))

    def test_no_as(self):
        query = self.table.select(self.table.c)
        try:
            Flavor.set(Flavor(no_as=True))
            self.assertEqual(str(query), 'SELECT "a"."c" FROM "t" "a"')
            self.assertEqual(query.params, ())
        finally:
            Flavor.set(Flavor())
