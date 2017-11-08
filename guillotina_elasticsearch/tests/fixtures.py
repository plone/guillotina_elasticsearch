from guillotina import testing
from guillotina.component import getUtility
from guillotina.interfaces import ICatalogUtility
from guillotina.tests.utils import ContainerRequesterAsyncContextManager
from guillotina_elasticsearch.tests.utils import run_elasticsearch_docker

import os
import pytest


def base_settings_configurator(settings):
    if 'applications' in settings:
        settings['applications'].append('guillotina_elasticsearch')
    else:
        settings['applications'] = ['guillotina_elasticsearch']

    settings['elasticsearch'] = {
        "index_name_prefix": "guillotina-",
        "connection_settings": {
            "endpoints": ["localhost:9200"],
            "sniffer_timeout": 0.5
        }
    }
    settings["utilities"] = []


testing.configure_with(base_settings_configurator)


@pytest.fixture(scope='session')
def elasticsearch():
    container = run_elasticsearch_docker()

    if os.environ.get('TESTING', '') == 'jenkins':
        setattr(elasticsearch, 'host', container.attrs['NetworkSettings']['IPAddress'])

    yield container

    container.remove(
        v=True,
        force=True
    )


def get_settings():
    settings = testing.get_settings()
    settings['elasticsearch']['connection_settings']['endpoints'] = [
        getattr(elasticsearch, 'host', 'localhost') + ':9200']
    return settings


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
                app_settings['elasticsearch']['connection_settings']['endpoints'] = [
                    getattr(elasticsearch, 'host', 'localhost') + ':9200']


@pytest.fixture(scope='function')
async def es_requester(elasticsearch, guillotina, loop):
    return ESRequester(guillotina, loop)
