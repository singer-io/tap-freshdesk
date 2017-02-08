#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path


setup(name='tap-freshdesk',
      version='0.2.0',
      description='Taps FreshDesk data',
      author='Stitch',
      url='https://github.com/stitchstreams/tap-freshdesk',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_freshdesk'],
      install_requires=[
          'stitchstream-python>=0.6.0',
          'requests==2.12.4',
          'backoff==1.3.2',
          'python-dateutil==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          tap-freshdesk=tap_freshdesk:main
      ''',
      packages=['tap_freshdesk'],
      package_data = {
          'tap_freshdesk': [
              'agents.json',
              'companies.json',
              'contacts.json',
              'conversations.json',
              'groups.json',
              'roles.json',
              'satisfaction_ratings.json',
              'tickets.json',
              'time_entries.json',
          ],
      }
)
