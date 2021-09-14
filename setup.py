#!/usr/bin/env python
# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.
from setuptools import setup, find_packages
import os
import re
import codecs


def read(fname):
    return codecs.open(
        os.path.join(os.path.dirname(__file__), fname), 'r', 'utf-8').read()


def get_version():
    init = read(os.path.join('sql', '__init__.py'))
    return re.search("__version__ = '([0-9.]*)'", init).group(1)


setup(name='python-sql',
    version=get_version(),
    description='Library to write SQL queries',
    long_description=read('README'),
    author='Tryton',
    author_email='python-sql@tryton.org',
    url='https://pypi.org/project/python-sql/',
    download_url='https://downloads.tryton.org/python-sql/',
    project_urls={
        "Bug Tracker": 'https://python-sql.tryton.org/',
        "Forum": 'https://discuss.tryton.org/tags/python-sql',
        "Source Code": 'https://hg.tryton.org/python-sql/',
        },
    keywords='SQL database query',
    packages=find_packages(),
    python_requires='>=3.5',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    license='BSD',
    test_suite='sql.tests',
    )
