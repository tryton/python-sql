#!/usr/bin/env python
# This file is part of python-sql.  The COPYRIGHT file at the top level of
# this repository contains the full copyright notices and license terms.
import codecs
import os
import re

from setuptools import find_packages, setup


def read(fname):
    return codecs.open(
        os.path.join(os.path.dirname(__file__), fname), 'r', 'utf-8').read()


def get_version():
    init = read(os.path.join('sql', '__init__.py'))
    return re.search("__version__ = '([0-9.]*)'", init).group(1)


setup(name='python-sql',
    version=get_version(),
    description='Library to write SQL queries',
    long_description=read('README.rst'),
    author='Tryton',
    author_email='foundation@tryton.org',
    url='https://pypi.org/project/python-sql/',
    download_url='https://downloads.tryton.org/python-sql/',
    project_urls={
        "Bug Tracker": 'https://bugs.tryton.org/python-sql',
        "Forum": 'https://discuss.tryton.org/tags/python-sql',
        "Source Code": 'https://code.tryton.org/python-sql',
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
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Programming Language :: Python :: 3.12',
        'Programming Language :: Python :: 3.13',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    license='BSD',
    )
