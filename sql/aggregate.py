# -*- coding: utf-8 -*-
#
# Copyright (c) 2011-2015, Cédric Krier
# Copyright (c) 2011-2015, B2CK
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#     * Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of the <organization> nor the
#       names of its contributors may be used to endorse or promote products
#       derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL <COPYRIGHT HOLDER> BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


from sql import Expression, Window

__all__ = ['Avg', 'BitAnd', 'BitOr', 'BoolAnd', 'BoolOr', 'Count', 'Every',
    'Max', 'Min', 'Stddev', 'Sum', 'Variance']


class Aggregate(Expression):
    __slots__ = ('expression', '_distinct', '_within', '_filter', '_window')
    _sql = ''

    def __init__(self, expression, distinct=False, within=None, filter_=None,
            window=None):
        # TODO order_by
        super(Aggregate, self).__init__()
        self.expression = expression
        self.distinct = distinct
        self.within = within
        self.filter_ = filter_
        self.window = window

    @property
    def distinct(self):
        return self._distinct

    @distinct.setter
    def distinct(self, value):
        assert isinstance(value, bool)
        self._distinct = value

    @property
    def within(self):
        return self._within

    @within.setter
    def within(self, value):
        if value is not None:
            if isinstance(value, Expression):
                value = [value]
            assert all(isinstance(col, Expression) for col in value)
        self._within = value

    @property
    def filter_(self):
        return self._filter

    @filter_.setter
    def filter_(self, value):
        from sql.operators import And, Or
        if value is not None:
            assert isinstance(value, (Expression, And, Or))
        self._filter = value

    @property
    def window(self):
        return self._window

    @window.setter
    def window(self, value):
        if value:
            assert isinstance(value, Window)
        self._window = value

    def __str__(self):
        quantifier = 'DISTINCT ' if self.distinct else ''
        aggregate = '%s(%s%s)' % (self._sql, quantifier, self.expression)
        within = ''
        if self.within:
            within = (' WITHIN GROUP (ORDER BY %s)'
                % ', '.join(map(str, self.within)))
        filter_ = ''
        if self.filter_:
            filter_ = ' FILTER (WHERE %s)' % self.filter_
        window = ''
        if self.window:
            window = ' OVER "%s"' % self.window.alias
        return aggregate + within + filter_ + window

    @property
    def params(self):
        p = list(self.expression.params)
        if self.within:
            for expression in self.within:
                p.extend(expression.params)
        if self.filter_:
            p.extend(self.filter_.params)
        return tuple(p)


class Avg(Aggregate):
    __slots__ = ()
    _sql = 'AVG'


class BitAnd(Aggregate):
    __slots__ = ()
    _sql = 'BIT_AND'


class BitOr(Aggregate):
    __slots__ = ()
    _sql = 'BIT_OR'


class BoolAnd(Aggregate):
    __slots__ = ()
    _sql = 'BOOL_AND'


class BoolOr(Aggregate):
    __slots__ = ()
    _sql = 'BOOL_OR'


class Count(Aggregate):
    __slots__ = ()
    _sql = 'COUNT'


class Every(Aggregate):
    __slots__ = ()
    _sql = 'EVERY'


class Max(Aggregate):
    __slots__ = ()
    _sql = 'MAX'


class Min(Aggregate):
    __slots__ = ()
    _sql = 'MIN'


class Stddev(Aggregate):
    __slots__ = ()
    _sql = 'Stddev'


class Sum(Aggregate):
    __slots__ = ()
    _sql = 'SUM'


class Variance(Aggregate):
    __slots__ = ()
    _sql = 'VARIANCE'
