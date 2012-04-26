#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.

# TODO NULLIF, GREATEST, LEAST

from sql import Column

__all__ = ['Case', 'Coalesce']


class Case(Column):
    __slots__ = ('whens', 'else_')
    _sql = ''
    table = ''
    name = ''

    def __init__(self, *args, **kwargs):
        self.whens = args
        self.else_ = kwargs.get('else_')

    def __str__(self):
        case = 'CASE '
        for cond, result in self.whens:
            case += 'WHEN ' + str(cond)
            if isinstance(result, basestring):
                result = '%s'
            case += ' THEN ' + str(result) + ' '
        if self.else_ is not None:
            else_ = self.else_
            if isinstance(self.else_, basestring):
                else_ = '%s'
            case += 'ELSE ' + str(else_) + ' '
        case += 'END'
        return case

    @property
    def params(self):
        p = ()
        for cond, result in self.whens:
            if hasattr(cond, 'params'):
                p += cond.params
            elif isinstance(cond, basestring):
                p += (cond,)
            if hasattr(result, 'params'):
                p += result.params
            elif isinstance(result, basestring):
                p += (result,)
        if self.else_ is not None:
            if hasattr(self.else_, 'params'):
                p += self.else_.params
            elif isinstance(self.else_, basestring):
                p += (self.else_,)
        return p


class Coalesce(Column):
    __slots__ = ('values')
    _sql = ''
    table = ''
    name = ''

    def __init__(self, *args):
        self.values = args

    def __str__(self):
        def format(value):
            if isinstance(value, basestring):
                return '%s'
            else:
                return str(value)
        return 'COALESCE(' + ', '.join(map(format, self.values)) + ')'

    @property
    def params(self):
        p = ()
        for value in self.values:
            if isinstance(value, basestring):
                p += (value,)
            elif hasattr(value, 'params'):
                p += value.params
        return p
