#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table
from sql.conditionals import Case


class TestConditionals(unittest.TestCase):
    table = Table('t')

    def test_case(self):
        case = Case((self.table.c1, 'foo'),
            (self.table.c2, 'bar'),
            else_=self.table.c3)
        self.assertEqual(str(case),
            'CASE WHEN "c1" THEN %s '
            'WHEN "c2" THEN %s '
            'ELSE "c3" END')
        self.assertEqual(case.params, ('foo', 'bar'))
