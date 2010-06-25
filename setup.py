#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import sys

try:
    from setuptools import setup, find_packages
except ImportError:
    from ez_setup import use_setuptools
    use_setuptools()
    from setuptools import setup, find_packages

import pycassa

setup(
      name = 'pycassa',
      description = 'Simple python library for Cassandra',
      long_description = pycassa.__doc__,
      version = pycassa.__version__,
      author = 'Jonathan Hseu',
      author_email = 'vomjom@vomjom.net',
      url = 'http://github.com/vomjom/pycassa',
      download_url = 'http://github.com/vomjom/pycassa',
      license = 'MIT',
      keywords = 'cassandra client database db distributed thrift',
      packages = ['pycassa'],
      scripts=['pycassaShell'],
      platforms = ['any'],
      install_requires = [
          'thrift',
          # FIXME: Are there dependable eggs/python packages for we can rely on?
          # 'python-cassandra'  # http://github.com/ieure/python-cassandra/
      ],
      test_suite="nose.collector",
      classifiers=[
        "Development Status :: 4 - Beta",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Topic :: Communications",
        "Topic :: Database",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Libraries :: Python Modules",
      ],
      )

