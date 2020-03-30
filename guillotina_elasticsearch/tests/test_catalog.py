from guillotina.component import get_adapter
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.tests.utils import create_content
from guillotina_elasticsearch.interfaces import DOC_TYPE
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

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
        doc = await conn.get(
            index=await im.get_index_name(), doc_type=DOC_TYPE, id="foobar"
        )
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
