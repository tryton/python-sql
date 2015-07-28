# -*- coding: utf-8 -*-
#
# Copyright (c) 2011-2013, Cédric Krier
# Copyright (c) 2011-2013, B2CK
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

from sql import Asc, Desc, NullsFirst, NullsLast, Column, Table, Literal
from sql import Flavor


class TestOrder(unittest.TestCase):
    column = Column(Table('t'), 'c')

    def test_asc(self):
        self.assertEqual(str(Asc(self.column)), '"c" ASC')

    def test_desc(self):
        self.assertEqual(str(Desc(self.column)), '"c" DESC')

    def test_nulls_first(self):
        self.assertEqual(str(NullsFirst(self.column)), '"c" NULLS FIRST')
        self.assertEqual(str(NullsFirst(Asc(self.column))),
            '"c" ASC NULLS FIRST')

    def test_nulls_last(self):
        self.assertEqual(str(NullsLast(self.column)), '"c" NULLS LAST')
        self.assertEqual(str(NullsLast(Asc(self.column))),
            '"c" ASC NULLS LAST')

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
