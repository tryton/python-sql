#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
import unittest

from sql import Literal


class TestLiteral(unittest.TestCase):
    def test_literal(self):
        literal = Literal(1)
        self.assertEqual(str(literal), '%s')
        self.assertEqual(literal.params, (1,))
