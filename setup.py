#!/usr/bin/env python

from setuptools import setup

setup(name='tap-freshdesk',
      version='0.11.0',
      description='Singer.io tap for extracting data from the Freshdesk API',
      author='Stitch',
      url='http://singer.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_freshdesk'],
      install_requires=[
          'singer-python==5.12.2',
          'requests==2.20.0',
          'backoff==1.8.0'
      ],
      extras_require={
          'dev': [
              'pylint',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-freshdesk=tap_freshdesk:main
      ''',
      packages=['tap_freshdesk'],
      package_data = {
          'tap_freshdesk': [
              'schemas/*.json'
          ],
      },
      include_package_data=True,
)
