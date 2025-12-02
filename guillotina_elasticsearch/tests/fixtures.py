from guillotina import app_settings
from guillotina import testing
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.tests.utils import ContainerRequesterAsyncContextManager
from guillotina_elasticsearch.interfaces import IConnectionFactoryUtility
from guillotina_elasticsearch.manager import default_settings
from guillotina_elasticsearch.tests.utils import cleanup_es

import os
import pytest


def elastic_search_analyzers_normalizers():
    settings_es = default_settings()

    common_analyzer = {
        "common_analyzer": {
            "tokenizer": "standard",
            "char_filter": ["html_strip"],
            "filter": ["lowercase", "asciifolding"],
        }
    }
    common_normalizer = {
        "common_normalizer": {
            "type": "custom",
            "char_filter": [],
            "filter": ["lowercase", "asciifolding"],
        }
    }
    settings_es["analysis"]["analyzer"].update(common_analyzer)
    settings_es["analysis"]["normalizer"] = common_normalizer
    return settings_es


def base_settings_configurator(settings):
    if "applications" not in settings:
        settings["applications"] = []

    if "guillotina.contrib.dbusers" not in settings["applications"]:
        settings["applications"].append("guillotina.contrib.dbusers")

    if "guillotina_elasticsearch" not in settings["applications"]:
        settings["applications"].append("guillotina_elasticsearch")

    if "guillotina_elasticsearch.testing" not in settings["applications"]:  # noqa
        settings["applications"].append("guillotina_elasticsearch.testing")

    settings["elasticsearch"] = {
        "index_name_prefix": "guillotina-",
        "index": elastic_search_analyzers_normalizers(),
        "connection_settings": {
            "hosts": [
                "http://{}:{}".format(
                    getattr(elasticsearch, "host", "localhost"),
                    getattr(elasticsearch, "port", "9200"),
                )
            ]
        },
    }

    settings["load_utilities"]["catalog"] = {
        "provides": "guillotina_elasticsearch.interfaces.IElasticSearchUtility",  # noqa
        "factory": "guillotina_elasticsearch.utility.ElasticSearchUtility",
        "settings": {},
    }


testing.configure_with(base_settings_configurator)


@pytest.fixture(scope="session")
def elasticsearch(es):
    host, port = es

    setattr(elasticsearch, "host", host)
    setattr(elasticsearch, "port", port)

    yield es


class ESRequester(ContainerRequesterAsyncContextManager):
    def __init__(self, guillotina, loop):
        super().__init__(guillotina)
        self.loop = loop

    async def __aenter__(self):
        # aioelasticsearch caches loop, we need to continue to reset it
        search = get_utility(ICatalogUtility)

        util = get_utility(IConnectionFactoryUtility)
        await util.close(search.loop)
        search.loop = self.loop

        if os.environ.get("TESTING", "") == "jenkins":
            if "elasticsearch" in app_settings:
                app_settings["elasticsearch"]["connection_settings"][
                    "hosts"
                ] = [  # noqa
                    "{}:{}".format(
                        getattr(elasticsearch, "host", "localhost"),
                        getattr(elasticsearch, "port", "9200"),
                    )
                ]
        return await super().__aenter__()


@pytest.fixture(scope="function")
async def es_requester(elasticsearch, guillotina, event_loop):
    # clean up all existing indexes
    es_host = "{}:{}".format(elasticsearch[0], elasticsearch[1])
    await cleanup_es(es_host, app_settings["elasticsearch"]["index_name_prefix"])
    return ESRequester(guillotina, event_loop)
