# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.

import unittest

from sql import AliasManager, Column, From, FromItem


class TestFromItem(unittest.TestCase):

    def test_from_item(self):
        from_item = FromItem()

        with AliasManager():
            self.assertFalse(from_item.has_alias)
            from_item.alias
            self.assertTrue(from_item.has_alias)

    def test_get_column(self):
        from_item = FromItem()

        foo = from_item.foo

        self.assertIsInstance(foo, Column)
        self.assertEqual(foo.name, 'foo')

    def test_get_invalid_column(self):
        from_item = FromItem()

        with self.assertRaises(AttributeError):
            from_item.__foo__

    def test_add(self):
        from_item1 = FromItem()
        from_item2 = FromItem()

        from_ = from_item1 + from_item2

        self.assertIsInstance(from_, From)
        self.assertEqual(from_, [from_item1, from_item2])

    def test_invalid_add(self):
        from_item = FromItem()

        with self.assertRaises(TypeError):
            from_item + 'foo'
