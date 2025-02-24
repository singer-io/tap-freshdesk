

from setuptools import setup, find_packages

setup(name='tap-freshdesk',
    version='0.1.0',
    description='Singer.io tap for extracting data from freshdesk API',
    author='Stitch Dev',
    url='http://singer.io',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_freshdesk'],
    install_requires= ['requests==2.32.3', 'singer-python==6.1.0', 'backoff==2.2.1'],
    entry_points='''
        [console_scripts]
        tap-freshdesk=tap_freshdesk:main
    ''',
    packages=find_packages(),
    package_data = {
        'tap_freshdesk': ['schemas/*.json'],
    },
    include_package_data=True,
)