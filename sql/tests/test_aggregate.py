# -*- coding: utf-8 -*-
#
# Copyright (c) 2011-2018, Cédric Krier
# Copyright (c) 2011-2018, B2CK
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

from sql import Table, Window, AliasManager, Flavor, Literal
from sql.aggregate import Avg, Count


class TestAggregate(unittest.TestCase):
    table = Table('t')

    def test_avg(self):
        avg = Avg(self.table.c)
        self.assertEqual(str(avg), 'AVG("c")')

        avg = Avg(self.table.a + self.table.b)
        self.assertEqual(str(avg), 'AVG(("a" + "b"))')

    def test_order_by_one_column(self):
        avg = Avg(self.table.a, order_by=self.table.b)
        self.assertEqual(str(avg), 'AVG("a" ORDER BY "b")')
        self.assertEqual(avg.params, ())

    def test_order_by_multiple_columns(self):
        avg = Avg(
            self.table.a, order_by=[self.table.b.asc, self.table.c.desc])
        self.assertEqual(str(avg), 'AVG("a" ORDER BY "b" ASC, "c" DESC)')
        self.assertEqual(avg.params, ())

    def test_within(self):
        avg = Avg(self.table.a, within=self.table.b)
        self.assertEqual(str(avg), 'AVG("a") WITHIN GROUP (ORDER BY "b")')
        self.assertEqual(avg.params, ())

    def test_filter(self):
        flavor = Flavor(filter_=True)
        Flavor.set(flavor)
        try:
            avg = Avg(self.table.a + 1, filter_=self.table.a > 0)
            self.assertEqual(
                str(avg), 'AVG(("a" + %s)) FILTER (WHERE ("a" > %s))')
            self.assertEqual(avg.params, (1, 0))
        finally:
            Flavor.set(Flavor())

    def test_filter_case(self):
        avg = Avg(self.table.a + 1, filter_=self.table.a > 0)
        self.assertEqual(
            str(avg), 'AVG(CASE WHEN ("a" > %s) THEN ("a" + %s) END)')
        self.assertEqual(avg.params, (0, 1))

    def test_filter_case_count_star(self):
        count = Count(Literal('*'), filter_=self.table.a > 0)
        self.assertEqual(
            str(count), 'COUNT(CASE WHEN ("a" > %s) THEN %s END)')
        self.assertEqual(count.params, (0, 1))

    def test_window(self):
        avg = Avg(self.table.c, window=Window([]))
        with AliasManager():
            self.assertEqual(str(avg), 'AVG("a"."c") OVER "b"')
        self.assertEqual(avg.params, ())

    def test_distinct(self):
        avg = Avg(self.table.c, distinct=True)
        self.assertEqual(str(avg), 'AVG(DISTINCT "c")')
        self.assertEqual(avg.params, ())
