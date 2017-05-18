from guillotina.component import getUtility
from guillotina.interfaces import ICatalogUtility
from guillotina.utils import get_content_path
from guillotina_elasticsearch.migration import Migrator
from guillotina_elasticsearch.tests.utils import add_content
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import asyncio
import pytest
import random


@pytest.mark.flaky(reruns=5)
async def _test_new_indexes_are_performed_during_migration(es_requester):
    async with await es_requester as requester:
        await add_content(requester)
        container, request, txn, tm = await setup_txn_on_container(requester)

        search = getUtility(ICatalogUtility)
        migrator = Migrator(search, container, force=True, request=request)
        await migrator.setup_next_index()
        await migrator.copy_to_next_index()

        await asyncio.sleep(1)
        await search.refresh(container, migrator.work_index_name)
        await search.refresh(container)
        await asyncio.sleep(1)
        num_docs = await search.get_doc_count(container, migrator.work_index_name)
        assert num_docs == await search.get_doc_count(container)

        await add_content(requester, base_id='foobar1-')

        await asyncio.sleep(1)
        await search.refresh(container, migrator.work_index_name)
        await search.refresh(container)
        await asyncio.sleep(1)
        num_docs = await search.get_doc_count(container, migrator.work_index_name)
        assert num_docs == await search.get_doc_count(container)


@pytest.mark.flaky(reruns=5)
async def _test_new_deletes_are_performed_during_migration(es_requester):
    async with await es_requester as requester:
        await add_content(requester)
        container, request, txn, tm = await setup_txn_on_container(requester)

        search = getUtility(ICatalogUtility)
        migrator = Migrator(search, container, force=True, request=request)
        await migrator.setup_next_index()
        await migrator.copy_to_next_index()

        await search.refresh(container, migrator.work_index_name)
        await search.refresh(container)
        num_docs = await search.get_doc_count(container, migrator.work_index_name)
        current_docs = await search.get_doc_count(container)
        assert num_docs == current_docs

        keys = await container.async_keys()
        key = random.choice(keys)
        ob = await container.async_get(key)
        keys = await ob.async_keys()
        key = random.choice(keys)
        ob = await ob.async_get(key)

        await search.remove(container, [(
            ob._p_oid, ob.type_name, get_content_path(ob)
        )], request=request, future=False)

        await search.refresh(container, migrator.work_index_name)
        await search.refresh(container)
        num_docs = await search.get_doc_count(container, migrator.work_index_name)
        current_count = await search.get_doc_count(container)
        assert num_docs == current_count
