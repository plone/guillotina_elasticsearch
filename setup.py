# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup

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
        'setuptools',
        'guillotina',
        'aioes>=0.7.0',
        'ujson',
    ],
    tests_require=[
        'pytest',
    ]
)
