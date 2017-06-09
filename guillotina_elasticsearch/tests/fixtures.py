from guillotina.component import getUtility
from guillotina.interfaces import ICatalogUtility
from guillotina.testing import TESTING_SETTINGS
from guillotina_elasticsearch.tests.utils import run_elasticsearch_docker

import os
import pytest

if 'applications' in TESTING_SETTINGS:
    TESTING_SETTINGS['applications'].append('guillotina_elasticsearch')
else:
    TESTING_SETTINGS['applications'] = ['guillotina_elasticsearch']

TESTING_SETTINGS['elasticsearch'] = {
    "index_name_prefix": "guillotina-",
    "connection_settings": {
        "endpoints": ["localhost:9200"],
        "sniffer_timeout": 0.5
    }
}


@pytest.fixture(scope='session')
def elasticsearch():
    container = run_elasticsearch_docker()

    if os.environ.get('TESTING', '') == 'jenkins':
        TESTING_SETTINGS['elasticsearch']['connection_settings']['endpoints'] = [
            container.attrs['NetworkSettings']['IPAddress'] + ':9200']

    yield container

    container.remove(
        v=True,
        force=True
    )


# XXX order of this import matters
from guillotina.tests.fixtures import ContainerRequesterAsyncContextManager  # noqa


class ESRequester(ContainerRequesterAsyncContextManager):
    def __init__(self, guillotina, loop):
        super().__init__(guillotina)

        # aioes caches loop, we need to continue to reset it
        search = getUtility(ICatalogUtility)
        search.loop = loop
        if search._conn:
            search._conn.close()
        search._conn = None
        from guillotina import app_settings
        if os.environ.get('TESTING', '') == 'jenkins':
            if 'elasticsearch' in app_settings:
                app_settings['elasticsearch']['connection_settings']['endpoints'] = \
                    TESTING_SETTINGS['elasticsearch']['connection_settings']['endpoints']


@pytest.fixture(scope='function')
async def es_requester(elasticsearch, guillotina, loop):
    return ESRequester(guillotina, loop)
