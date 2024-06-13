from guillotina.component import get_adapter
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.tests.utils import create_content
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import asyncio
import json
import pytest


pytestmark = [pytest.mark.asyncio]


async def test_index(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        search = get_utility(ICatalogUtility)
        current_count = await search.get_doc_count(container)
        await search.index(
            container, {"foobar": {"title": "foobar", "type_name": "Item"}}
        )
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count + 1


async def test_update(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        search = get_utility(ICatalogUtility)
        current_count = await search.get_doc_count(container)
        await search.index(
            container, {"foobar": {"title": "foobar", "type_name": "Item"}}
        )
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count + 1
        await search.update(
            container, {"foobar": {"title": "foobar-updated", "type_name": "Item"}}
        )
        await search.refresh(container)
        im = get_adapter(container, IIndexManager)
        conn = search.get_connection()
        doc = await conn.get(index=await im.get_index_name(), id="foobar")
        assert doc["_source"]["title"] == "foobar-updated"


async def test_delete(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        search = get_utility(ICatalogUtility)
        current_count = await search.get_doc_count(container)
        await search.index(
            container, {"foobar": {"title": "foobar", "type_name": "Item"}}
        )
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count + 1

        ob = create_content(id="foobar")
        ob.__uuid__ = "foobar"

        await search.remove(container, [ob])
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count


@pytest.mark.app_settings(
    {
        "applications": [
            "guillotina",
            "guillotina_elasticsearch",
            "guillotina_elasticsearch.tests.test_package",
        ]
    }
)
async def test_update_catalog_mappings_settings(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        search = get_utility(ICatalogUtility)
        index_manager = get_adapter(container, IIndexManager)
        real_index_name = await index_manager.get_real_index_name()
        conn = search.get_connection()
        mappings_es = await conn.indices.get_mapping(index=real_index_name)
        settings_es = await conn.indices.get_settings(index=real_index_name)
        analysis_settings = settings_es[real_index_name]["settings"]["index"][
            "analysis"
        ]
        # Makes sure that a custom index field from a cutsom guillotina
        # app is added to the mappings
        assert (
            mappings_es[real_index_name]["mappings"]["properties"]["item_text"]["type"]
            == "text"
        )
        assert analysis_settings["analyzer"]["common_analyzer"]["filter"] == [
            "lowercase",
            "asciifolding",
        ]
        # Let's add a new mapping rule
        new_mappings = {
            "properties": {"foo_new_mapping": {"type": "text", "store": True}}
        }
        # Let's modify all the filters for analyzers and normalizers and add a new one: common_analyzer2
        new_settings = {
            "index": {
                "analysis": {
                    "analyzer": {
                        "common_analyzer": {
                            "tokenizer": "standard",
                            "char_filter": [],
                            "filter": ["asciifolding"],
                        },
                        "common_analyzer2": {
                            "tokenizer": "standard",
                            "char_filter": [],
                            "filter": ["asciifolding"],
                        },
                    },
                    "normalizer": {
                        "common_normalizer": {
                            "type": "custom",
                            "char_filter": [],
                            "filter": ["asciifolding"],
                        }
                    },
                }
            }
        }
        payload = {"settings": new_settings, "mappings": new_mappings}
        await search.update_catalog(container, payload)
        new_mappings_es = await conn.indices.get_mapping(index=real_index_name)
        new_settings_es = await conn.indices.get_settings(index=real_index_name)
        new_analysis_settings = new_settings_es[real_index_name]["settings"]["index"][
            "analysis"
        ]
        # Makes sure all changes had been done
        assert (
            new_mappings_es[real_index_name]["mappings"]["properties"][
                "foo_new_mapping"
            ]["type"]
            == "text"
        )
        assert new_analysis_settings["analyzer"]["common_analyzer"]["filter"] == [
            "asciifolding"
        ]
        assert new_analysis_settings["analyzer"]["common_analyzer2"]["filter"] == [
            "asciifolding"
        ]
        assert new_analysis_settings["normalizer"]["common_normalizer"]["filter"] == [
            "asciifolding"
        ]
        assert (
            "tokenizer"
            in new_settings_es[real_index_name]["settings"]["index"]["analysis"]
        )
        # Makes sure that older mappings are not deleted
        for mapping in mappings_es[real_index_name]["mappings"]["properties"]:
            assert mapping in new_mappings_es[real_index_name]["mappings"]["properties"]
        # Makes sure foo_new_mapping has been added
        assert new_mappings_es[real_index_name]["mappings"]["properties"][
            "foo_new_mapping"
        ] == {"type": "text", "store": True}

        # Makes sure that custom mappings and settings are preserved
        # when calling update without payload
        await search.update_catalog(container)
        new_mappings_es = await conn.indices.get_mapping(index=real_index_name)
        assert new_mappings_es[real_index_name]["mappings"]["properties"][
            "foo_new_mapping"
        ] == {"type": "text", "store": True}
        for mapping in mappings_es[real_index_name]["mappings"]["properties"]:
            assert mapping in new_mappings_es[real_index_name]["mappings"]["properties"]

        # The same for settings
        new_settings_es = await conn.indices.get_settings(index=real_index_name)
        new_analysis_settings = new_settings_es[real_index_name]["settings"]["index"][
            "analysis"
        ]
        assert new_analysis_settings["analyzer"]["common_analyzer2"]["filter"] == [
            "asciifolding"
        ]
        # Let's make sure we can search and nothing is broken
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "FooContent",
                    "title": "Item",
                    "id": "item",
                    "item_keyword": "foo_kéyword",
                    "item_text": "foo_item",
                }
            ),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?item_keyword=foo_keyword&_metadata=item_keyword",
            headers={"X-Wait": "10"},
        )
        assert status == 200
        assert resp["items_total"] == 1
