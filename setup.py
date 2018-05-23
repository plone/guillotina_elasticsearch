# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup


test_requires = [
    'pytest',
    'docker',
    'backoff',
    'psycopg2',
    'pytest-asyncio<=0.5.0',
    'pytest-cov',
    'pytest-aiohttp',
    'pytest-rerunfailures',
    'pytest-docker-fixtures>=1.2.2'
]


setup(
    name='guillotina_elasticsearch',
    description='elasticsearch catalog support for guillotina',
    keywords='search async guillotina elasticsearch',
    author='Ramon Navarro Bosch & Nathan Van Gheem',
    author_email='ramon@plone.org',
    version=open('VERSION').read().strip(),
    long_description=(open('README.rst').read() + '\n' +
                      open('CHANGELOG.rst').read()),
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    url='https://pypi.python.org/pypi/guillotina_elasticsearch',
    license='GPL version 3',
    setup_requires=[
        'pytest-runner',
    ],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=['ez_setup']),
    install_requires=[
        'guillotina>=3.3.0',
        'aioelasticsearch',
        'ujson',
        'lru-dict',
        'backoff'
    ],
    tests_require=test_requires,
    extras_require={
        'test': test_requires
    }
)
