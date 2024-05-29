# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.

import unittest

from sql import Expression


class TestExpression(unittest.TestCase):

    def test_str(self):
        with self.assertRaises(NotImplementedError):
            str(Expression())

    def test_params(self):
        with self.assertRaises(NotImplementedError):
            Expression().params
