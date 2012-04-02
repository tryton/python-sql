#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.

# TODO COALESCE, NULLIF, GREATEST, LEAST

from sql import Column

__all__ = ['Case']


class Case(Column):
    __slots__ = ('__whens', '__else')
    _sql = ''
    table = ''
    name = ''

    def __init__(self, *args, **kwargs):
        self.__whens = args
        self.__else = kwargs.get('else_')

    def __str__(self):
        case = 'CASE '
        for cond, result in self.__whens:
            case += 'WHEN ' + str(cond)
            if isinstance(result, basestring):
                result = '%s'
            case += ' THEN ' + str(result) + ' '
        if self.__else is not None:
            else_ = self.__else
            if isinstance(self.__else, basestring):
                else_ = '%s'
            case += 'ELSE ' + str(else_) + ' '
        case += 'END'
        return case

    @property
    def params(self):
        p = ()
        for cond, result in self.__whens:
            if hasattr(cond, 'params'):
                p += cond.params
            elif isinstance(cond, basestring):
                p += (cond,)
            if hasattr(result, 'params'):
                p += result.params
            elif isinstance(result, basestring):
                p += (result,)
        if self.__else is not None:
            if hasattr(self.__else, 'params'):
                p += self.__else.params
            elif isinstance(self.__else, basestring):
                p += (self.__else,)
        return p
