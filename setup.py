#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import os
import sys

def read(*rnames):
    return open(os.path.join(os.path.dirname(__file__), *rnames)).read()

if float('%d.%d' % sys.version_info[:2]) < 2.6 or float('%d.%d' % sys.version_info[:2]) >= 3.0:
    sys.stderr.write("Your Python version %d.%d.%d is not supported.\n" %
                     sys.version_info[:3])
    sys.stderr.write("CAS Python SDK requires Python between 2.6 and 3.0.\n")
    sys.exit(1)

setup(
    name='cassdk',
    version='0.0.1',
    description='Python SDK for Tencent CAS (Cloud Archive Service)',
    author='Tencent CAS',
    url='http://www.qcloud.com/',
    packages=['cas', 'cas.ease'],
    license='MIT License',
    install_requires=[
        'pyaml',
        'ordereddict',
    ],
)
