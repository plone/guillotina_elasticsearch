from guillotina import testing
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina_elasticsearch.tests.utils import cleanup_es
from guillotina.tests.utils import ContainerRequesterAsyncContextManager

import os
import pytest


def base_settings_configurator(settings):
    if 'applications' in settings:
        settings['applications'].append('guillotina_elasticsearch')
    else:
        settings['applications'] = ['guillotina_elasticsearch']

    settings['applications'].append(
        'guillotina_elasticsearch.tests.package')

    settings['elasticsearch'] = {
        "index_name_prefix": "guillotina-",
        "connection_settings": {
            "hosts": ['{}:{}'.format(
                getattr(elasticsearch, 'host', 'localhost'),
                getattr(elasticsearch, 'port', '9200'),
            )],
            "sniffer_timeout": None
        }
    }
    settings["utilities"] = []


testing.configure_with(base_settings_configurator)


@pytest.fixture(scope='session')
def elasticsearch(es):
    host, port = es

    setattr(elasticsearch, 'host', host)
    setattr(elasticsearch, 'port', port)

    yield es


class ESRequester(ContainerRequesterAsyncContextManager):
    def __init__(self, guillotina, loop):
        super().__init__(guillotina)

        # aioelasticsearch caches loop, we need to continue to reset it
        search = get_utility(ICatalogUtility)
        search.loop = loop
        if search._conn:
            search._conn.close()
        search._conn = None
        from guillotina import app_settings
        if os.environ.get('TESTING', '') == 'jenkins':
            if 'elasticsearch' in app_settings:
                app_settings['elasticsearch']['connection_settings']['hosts'] = [  # noqa
                    '{}:{}'.format(
                        getattr(elasticsearch, 'host', 'localhost'),
                        getattr(elasticsearch, 'port', '9200'),
                    )]


@pytest.fixture(scope='function')
async def es_requester(elasticsearch, guillotina, loop):
    # clean up all existing indexes
    es_host = '{}:{}'.format(
        elasticsearch[0], elasticsearch[1])
    await cleanup_es(es_host)
    return ESRequester(guillotina, loop)
