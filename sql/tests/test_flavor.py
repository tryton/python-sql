# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.

import unittest

from sql import Flavor


class TestFlavor(unittest.TestCase):

    def test(self):
        Flavor()

    def test_limitstyle(self):
        flavor = Flavor(limitstyle='rownum')

        self.assertEqual(flavor.limitstyle, 'rownum')

    def test_invalid_limitstyle(self):
        with self.assertRaises(ValueError):
            Flavor(limitstyle='foo')

    def test_max_limit(self):
        flavor = Flavor(max_limit=42)

        self.assertEqual(flavor.max_limit, 42)

    def test_invalid_max_limit(self):
        with self.assertRaises(ValueError):
            Flavor(max_limit='foo')

    def test_paramstyle_format(self):
        flavor = Flavor(paramstyle='format')

        self.assertEqual(flavor.paramstyle, 'format')
        self.assertEqual(flavor.param, '%s')

    def test_paramstyle_qmark(self):
        flavor = Flavor(paramstyle='qmark')

        self.assertEqual(flavor.paramstyle, 'qmark')
        self.assertEqual(flavor.param, '?')

    def test_invalid_paramstyle(self):
        with self.assertRaises(ValueError):
            Flavor(paramstyle='foo')
