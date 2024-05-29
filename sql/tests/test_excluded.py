# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.

import unittest

from sql import Excluded


class TestExcluded(unittest.TestCase):

    def test_alias(self):
        self.assertEqual(Excluded.alias, 'EXCLUDED')

    def test_has_alias(self):
        self.assertFalse(Excluded.has_alias)
