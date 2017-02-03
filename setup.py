#!/usr/bin/env python

from setuptools import setup, find_packages
import os.path


with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'VERSION')) as f:
    version = f.read().strip()


setup(name='tap-freshdesk',
      version=version,
      description='Taps FreshDesk data',
      author='Stitch',
      url='https://github.com/stitchstreams/stream-freshdesk',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_freshdesk'],
      install_requires=[
          'stitchstream-python>=0.5.0',
          'requests==2.12.4',
          'backoff==1.3.2',
          'python-dateutil==2.6.0',
      ],
      entry_points='''
          [console_scripts]
          tap-freshdesk=tap_freshdesk:main
      ''',
      packages=['schemas'],
      package_data = {
          'schemas': [
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
          '': [
              'VERSION',
          ]
      }
)
