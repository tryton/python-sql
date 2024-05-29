# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.

import unittest

from sql import CombiningQuery, From, Table


class TestFrom(unittest.TestCase):

    def test_add(self):
        t1 = Table('t1')
        t2 = Table('t2')
        from_ = From([t1]) + t2

        self.assertEqual(from_, [t1, t2])

    def test_invalid_add(self):
        with self.assertRaises(TypeError):
            From([Table('t')]) + 'foo'

    def test_invalid_add_combining_query(self):
        with self.assertRaises(TypeError):
            From([Table('t')]) + CombiningQuery(
                Table('t1').select(), Table('t2').select())
