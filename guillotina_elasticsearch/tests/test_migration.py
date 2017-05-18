from guillotina import app_settings
from guillotina.component import getUtility
from guillotina_elasticsearch.schema import get_mappings
from guillotina.interfaces import ICatalogUtility
from guillotina_elasticsearch.manager import DEFAULT_SETTINGS
from guillotina_elasticsearch.migration import Migrator
from guillotina_elasticsearch.tests.utils import add_content
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import asyncio
import json
import time


async def test_migrate_while_content_getting_added(es_requester):
    async with await es_requester as requester:
        await add_content(requester)

        container, request, txn, tm = await setup_txn_on_container(requester)

        search = getUtility(ICatalogUtility)
        await search.refresh(container)

        current_count = await search.get_doc_count(container)

        migrator = Migrator(search, container, force=True)
        add_content_task = asyncio.ensure_future(add_content(requester, base_id='foo-'))
        reindex_task = asyncio.ensure_future(migrator.run_migration())

        await asyncio.wait([reindex_task, add_content_task])
        await search.refresh(container)

        current_count = await search.get_doc_count(container)

        await tm.abort(txn=txn)


async def test_migrate_get_all_uids(es_requester):
    async with await es_requester as requester:
        await add_content(requester)

        container, request, txn, tm = await setup_txn_on_container(requester)

        search = getUtility(ICatalogUtility)
        await search.refresh(container)

        current_count = await search.get_doc_count(container)

        migrator = Migrator(search, container, force=True)
        uids = await migrator.get_all_uids()

        assert len(uids) == current_count

        await tm.abort(txn=txn)


async def test_removes_orphans():
    pass


async def test_fixes_missing():
    pass


async def test_updates_changed_mapping_fields():
    pass


async def test_updates_index_data():
    pass


async def test_flush_pushes_data_to_es():
    pass


async def test_calculate_mapping_diff(es_requester):
    async with await es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = getUtility(ICatalogUtility)

        migrator = Migrator(search, container, force=True)
        version, new_index_name = await migrator.create_next_index()
        migrator.next_index_name = new_index_name

        mappings = get_mappings()
        index_settings = DEFAULT_SETTINGS.copy()
        index_settings.update(app_settings.get('index', {}))

        # tweak mappings so we can get the diff...
        for key, value in mappings.items():
            # need to modify on *all* or it won't work with ES..
            if 'creators' in value['properties']:
                value['properties']['creators']['type'] = 'text'
        mappings['Item']['properties']['foobar'] = {'type': 'keyword', 'index': True}

        await search.conn.indices.close(new_index_name)
        await search.conn.indices.put_settings(index_settings, new_index_name)
        for key, value in mappings.items():
            await search.conn.indices.put_mapping(new_index_name, key, value)
        await search.conn.indices.open(new_index_name)

        diff = await migrator.calculate_mapping_diff()
        assert len(diff) == (len(mappings) - 1)
        assert len(diff['Folder']) == 1
        assert len(diff['Item']) == 2


async def test_updates_index_name(es_requester):
    async with await es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = getUtility(ICatalogUtility)
        existing_index = await search.get_real_index_name(container)
        assert await search.conn.indices.exists(existing_index)
        migrator = Migrator(search, container, force=True)
        await migrator.run_migration()
        assert not await search.conn.indices.exists(existing_index)
        assert search.conn.indices.exists(migrator.next_index_name)
        assert await search.get_real_index_name(container) == migrator.next_index_name


async def test_moves_docs_over(es_requester):
    async with await es_requester as requester:
        await add_content(requester)
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = getUtility(ICatalogUtility)

        await search.refresh(container)
        current_count = await search.get_doc_count(container)

        migrator = Migrator(search, container, force=True)
        await migrator.run_migration()

        assert await search.get_real_index_name(container) == migrator.next_index_name
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count


async def test_create_next_index(es_requester):
    async with await es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = getUtility(ICatalogUtility)
        migrator = Migrator(search, container, force=True)
        version, name = await migrator.create_next_index()
        assert version == 2
        assert name == 'guillotina-guillotina_2'
