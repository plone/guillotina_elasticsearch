# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup


test_requires = [
    "async_asgi_testclient",
    "pytest>=5.0",
    "pytest-asyncio==0.18.3",
    "coverage",
    "pytest-cov",
    "pytest-docker-fixtures[pg]>=1.3.0",
    "prometheus-client>=0.9.0",  # TODO: remove
    "docker>=5.0.0,<6.0.0"
]


setup(
    name="guillotina_elasticsearch",
    description="elasticsearch catalog support for guillotina",
    keywords="search async guillotina elasticsearch",
    author="Ramon Navarro Bosch & Nathan Van Gheem",
    author_email="ramon@plone.org",
    version=open("VERSION").read().strip(),
    long_description=(open("README.rst").read() + "\n" + open("CHANGELOG.rst").read()),
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    url="https://github.com/plone/guillotina_elasticsearch",
    license="GPL version 3",
    setup_requires=["pytest-runner"],
    zip_safe=True,
    include_package_data=True,
    package_data={"": ["*.txt", "*.rst"], "guillotina_elasticsearch": ["py.typed"]},
    packages=find_packages(exclude=["ez_setup"]),
    install_requires=[
        "guillotina>=6.0.0a16",
        "elasticsearch[async]>=7.8.0,<8.0.0",
        "mypy_extensions",
        "lru-dict",
        "backoff"
    ],
    tests_require=test_requires,
    extras_require={"test": test_requires},
)
