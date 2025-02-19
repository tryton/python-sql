python-sql
==========

python-sql is a library to write SQL queries in a pythonic way.

Nutshell
--------

Import::

    >>> from sql import *
    >>> from sql.aggregate import *
    >>> from sql.conditionals import *

Simple selects::

    >>> user = Table('user')
    >>> select = user.select()
    >>> tuple(select)
    ('SELECT * FROM "user" AS "a"', ())

    >>> select = user.select(user.name)
    >>> tuple(select)
    ('SELECT "a"."name" FROM "user" AS "a"', ())

    >>> select = user.select(Count(Literal(1)))
    >>> tuple(select)
    ('SELECT COUNT(%s) FROM "user" AS "a"', (1,))

    >>> select = user.select(user.name, distinct=True)
    >>> tuple(select)
    ('SELECT DISTINCT "a"."name" FROM "user" AS "a"', ())

    >>> select = user.select(user.id, user.name)
    >>> tuple(select)
    ('SELECT "a"."id", "a"."name" FROM "user" AS "a"', ())

Select with where condition::

    >>> select.where = user.name == 'foo'
    >>> tuple(select)
    ('SELECT "a"."id", "a"."name" FROM "user" AS "a" WHERE ("a"."name" = %s)', ('foo',))

    >>> select.where = (user.name == 'foo') & (user.active == True)
    >>> tuple(select)
    ('SELECT "a"."id", "a"."name" FROM "user" AS "a" WHERE (("a"."name" = %s) AND ("a"."active" = %s))', ('foo', True))
    >>> select.where = user.name == user.login
    >>> tuple(select)
    ('SELECT "a"."id", "a"."name" FROM "user" AS "a" WHERE ("a"."name" = "a"."login")', ())

Select with join::

    >>> join = user.join(Table('user_group'))
    >>> join.condition = join.right.user == user.id
    >>> select = join.select(user.name, join.right.group)
    >>> tuple(select)
    ('SELECT "a"."name", "b"."group" FROM "user" AS "a" INNER JOIN "user_group" AS "b" ON ("b"."user" = "a"."id")', ())

Select with multiple joins::

    >>> join1 = user.join(Table('user'))
    >>> join2 = join1.join(Table('user'))
    >>> select = join2.select(user.id, join1.right.id, join2.right.id)
    >>> tuple(select)
    ('SELECT "a"."id", "b"."id", "c"."id" FROM "user" AS "a" INNER JOIN "user" AS "b" INNER JOIN "user" AS "c"', ())

Select with group_by::

    >>> invoice = Table('invoice')
    >>> select = invoice.select(Sum(invoice.amount), invoice.currency,
    ...         group_by=invoice.currency)
    >>> tuple(select)
    ('SELECT SUM("a"."amount"), "a"."currency" FROM "invoice" AS "a" GROUP BY "a"."currency"', ())

Select with output name::

    >>> tuple(user.select(user.name.as_('First Name')))
    ('SELECT "a"."name" AS "First Name" FROM "user" AS "a"', ())

Select with order_by::

    >>> tuple(user.select(order_by=user.date))
    ('SELECT * FROM "user" AS "a" ORDER BY "a"."date"', ())
    >>> tuple(user.select(order_by=Asc(user.date)))
    ('SELECT * FROM "user" AS "a" ORDER BY "a"."date" ASC', ())
    >>> tuple(user.select(order_by=(user.date.asc, user.id.desc)))
    ('SELECT * FROM "user" AS "a" ORDER BY "a"."date" ASC, "a"."id" DESC', ())

Select with sub-select::

    >>> user_group = Table('user_group')
    >>> subselect = user_group.select(user_group.user,
    ...     where=user_group.active == True)
    >>> user = Table('user')
    >>> tuple(user.select(user.id, where=user.id.in_(subselect)))
    ('SELECT "a"."id" FROM "user" AS "a" WHERE ("a"."id" IN (SELECT "b"."user" FROM "user_group" AS "b" WHERE ("b"."active" = %s)))', (True,))
    >>> tuple(subselect.select(subselect.user))
    ('SELECT "a"."user" FROM (SELECT "b"."user" FROM "user_group" AS "b" WHERE ("b"."active" = %s)) AS "a"', (True,))

Select on other schema::

    >>> other_table = Table('user', 'myschema')
    >>> tuple(other_table.select())
    ('SELECT * FROM "myschema"."user" AS "a"', ())

Insert query with default values::

    >>> tuple(user.insert())
    ('INSERT INTO "user" DEFAULT VALUES', ())

Insert query with values::

    >>> tuple(user.insert(columns=[user.name, user.login],
    ...         values=[['Foo', 'foo']]))
    ('INSERT INTO "user" ("name", "login") VALUES (%s, %s)', ('Foo', 'foo'))
    >>> tuple(user.insert(columns=[user.name, user.login],
    ...         values=[['Foo', 'foo'], ['Bar', 'bar']]))
    ('INSERT INTO "user" ("name", "login") VALUES (%s, %s), (%s, %s)', ('Foo', 'foo', 'Bar', 'bar'))

Insert query with query::

    >>> passwd = Table('passwd')
    >>> select = passwd.select(passwd.login, passwd.passwd)
    >>> tuple(user.insert(values=select))
    ('INSERT INTO "user" SELECT "a"."login", "a"."passwd" FROM "passwd" AS "a"', ())

Update query with values::

    >>> tuple(user.update(columns=[user.active], values=[True]))
    ('UPDATE "user" AS "a" SET "active" = %s', (True,))
    >>> tuple(invoice.update(columns=[invoice.total], values=[invoice.amount + invoice.tax]))
    ('UPDATE "invoice" AS "a" SET "total" = ("a"."amount" + "a"."tax")', ())

Update query with where condition::

    >>> tuple(user.update(columns=[user.active], values=[True],
    ...          where=user.active == False))
    ('UPDATE "user" AS "a" SET "active" = %s WHERE ("a"."active" = %s)', (True, False))

Update query with from list::

    >>> group = Table('user_group')
    >>> tuple(user.update(columns=[user.active], values=[group.active],
    ...         from_=[group], where=user.id == group.user))
    ('UPDATE "user" AS "b" SET "active" = "a"."active" FROM "user_group" AS "a" WHERE ("b"."id" = "a"."user")', ())

Delete query::

    >>> tuple(user.delete())
    ('DELETE FROM "user"', ())

Delete query with where condition::

    >>> tuple(user.delete(where=user.name == 'foo'))
    ('DELETE FROM "user" WHERE ("name" = %s)', ('foo',))

Delete query with sub-query::

    >>> tuple(user.delete(
    ...             where=user.id.in_(user_group.select(user_group.user))))
    ('DELETE FROM "user" WHERE ("id" IN (SELECT "a"."user" FROM "user_group" AS "a"))', ())

Flavors::

    >>> select = user.select()
    >>> select.offset = 10
    >>> Flavor.set(Flavor())
    >>> tuple(select)
    ('SELECT * FROM "user" AS "a" OFFSET %s', (10,))
    >>> Flavor.set(Flavor(max_limit=18446744073709551615))
    >>> tuple(select)
    ('SELECT * FROM "user" AS "a" LIMIT 18446744073709551615 OFFSET %s', (10,))
    >>> Flavor.set(Flavor(max_limit=-1))
    >>> tuple(select)
    ('SELECT * FROM "user" AS "a" LIMIT -1 OFFSET %s', (10,))

Limit style::

    >>> select = user.select(limit=10, offset=20)
    >>> Flavor.set(Flavor(limitstyle='limit'))
    >>> tuple(select)
    ('SELECT * FROM "user" AS "a" LIMIT %s OFFSET %s', (10, 20))
    >>> Flavor.set(Flavor(limitstyle='fetch'))
    >>> tuple(select)
    ('SELECT * FROM "user" AS "a" OFFSET (%s) ROWS FETCH FIRST (%s) ROWS ONLY', (20, 10))
    >>> Flavor.set(Flavor(limitstyle='rownum'))
    >>> tuple(select)
    ('SELECT "a".* FROM (SELECT "b".*, ROWNUM AS "rnum" FROM (SELECT * FROM "user" AS "c") AS "b" WHERE (ROWNUM <= %s)) AS "a" WHERE ("rnum" > %s)', (30, 20))

qmark style::

    >>> Flavor.set(Flavor(paramstyle='qmark'))
    >>> select = user.select()
    >>> select.where = user.name == 'foo'
    >>> tuple(select)
    ('SELECT * FROM "user" AS "a" WHERE ("a"."name" = ?)', ('foo',))

numeric style::

    >>> Flavor.set(Flavor(paramstyle='format'))
    >>> select = user.select()
    >>> select.where = user.name == 'foo'
    >>> format2numeric(*select)
    ('SELECT * FROM "user" AS "a" WHERE ("a"."name" = :0)', ('foo',))
