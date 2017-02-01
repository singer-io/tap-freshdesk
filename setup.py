#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path

setup(name='stream-freshdesk',
      version='0.1.0',
      description='Streams FreshDesk data',
      author='Stitch',
      url='https://github.com/stitchstreams/stream-freshdesk',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['stream_freshdesk'],
      install_requires=[
          'stitchstream-python>=0.5.0',
          'requests==2.12.4',
          'backoff==1.3.2',
          'python-dateutil==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          stream-freshdesk=stream_freshdesk:main
      ''',
      packages=['stream_freshdesk'],
      package_data = {
          'stream_freshdesk': [
              'agents.json',
              'companies.json',
              'contacts.json',
              'conversations.json',
              'groups.json',
              'roles.json',
              'satisfaction_ratings.json',
              'tickets.json',
              'time_entries.json',
          ]
      }
)
