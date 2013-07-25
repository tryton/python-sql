#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.

from sql import Expression, Flavor

__all__ = ['Case', 'Coalesce', 'NullIf', 'Greatest', 'Least']


class Conditional(Expression):
    __slots__ = ()
    _sql = ''
    table = ''
    name = ''

    @staticmethod
    def _format(value):
        if isinstance(value, Expression):
            return str(value)
        else:
            return Flavor().get().param


class Case(Conditional):
    __slots__ = ('whens', 'else_')

    def __init__(self, *whens, **kwargs):
        self.whens = whens
        self.else_ = kwargs.get('else_')

    def __str__(self):
        case = 'CASE '
        for cond, result in self.whens:
            case += 'WHEN %s THEN %s ' % (
                self._format(cond), self._format(result))
        if self.else_ is not None:
            case += 'ELSE %s ' % self._format(self.else_)
        case += 'END'
        return case

    @property
    def params(self):
        p = ()
        for cond, result in self.whens:
            if isinstance(cond, Expression):
                p += cond.params
            else:
                p += (cond,)
            if isinstance(result, Expression):
                p += result.params
            else:
                p += (result,)
        if self.else_ is not None:
            if isinstance(self.else_, Expression):
                p += self.else_.params
            else:
                p += (self.else_,)
        return p


class Coalesce(Conditional):
    __slots__ = ('values')
    _conditional = 'COALESCE'

    def __init__(self, *values):
        self.values = values

    def __str__(self):
        return (self._conditional
            + '(' + ', '.join(map(self._format, self.values)) + ')')

    @property
    def params(self):
        p = ()
        for value in self.values:
            if isinstance(value, Expression):
                p += value.params
            else:
                p += (value,)
        return p


class NullIf(Coalesce):
    __slots__ = ()
    _conditional = 'NULLIF'


class Greatest(Coalesce):
    __slots__ = ()
    _conditional = 'GREATEST'


class Least(Coalesce):
    __slots__ = ()
    _conditional = 'LEAST'
