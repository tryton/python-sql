# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.

import unittest

from sql import Grouping


class TestGrouping(unittest.TestCase):

    def test_invalid_sets(self):
        with self.assertRaises(ValueError):
            Grouping('foo')
