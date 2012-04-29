#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Table, Flavor
from sql.functions import Function, Abs


class TestFunctions(unittest.TestCase):
    table = Table('t')

    def test_abs(self):
        abs_ = Abs(self.table.c1)
        self.assertEqual(str(abs_), 'ABS("c1")')
        self.assertEqual(abs_.params, ())

        abs_ = Abs(-12)
        self.assertEqual(str(abs_), 'ABS(-12)')
        self.assertEqual(abs_.params, ())

    def test_mapping(self):
        class MyAbs(Function):
            _function = 'MY_ABS'
            params = ('test',)

        abs_ = Abs(self.table.c1)
        flavor = Flavor(function_mapping={
                Abs: MyAbs,
                })
        Flavor.set(flavor)
        try:
            self.assertEqual(str(abs_), 'MY_ABS("c1")')
            self.assertEqual(abs_.params, ('test',))
        finally:
            Flavor.set(Flavor())
