#!/usr/bin/env python
#This file is part of python-sql.  The COPYRIGHT file at the top level of
#this repository contains the full copyright notices and license terms.
from setuptools import setup, find_packages
import os
import sql


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(name='python-sql',
    version=sql.__version__,
    description='Library to write SQL queries',
    long_description=read('README'),
    author='B2CK',
    author_email='info@b2ck.com',
    url='http://code.google.com/p/python-sql/',
    download_url='http://code.google.com/p/python-sql/downloads/',
    packages=find_packages(),
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Python Modules',
        ],
    license='LGPL-3',
    test_suite='sql.tests',
    )
