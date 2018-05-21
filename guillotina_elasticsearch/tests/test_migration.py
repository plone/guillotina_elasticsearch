from guillotina.component import get_adapter
from guillotina.db.oid import get_short_oid
from guillotina.component import get_utility
from guillotina.component import globalregistry as gr
from guillotina.event import notify
from guillotina.events import ObjectRemovedEvent
from guillotina.interfaces import IAnnotations
from guillotina.interfaces import ICatalogUtility
from guillotina.registry import REGISTRY_DATA_KEY
from guillotina.tests.utils import create_content
from guillotina.transactions import managed_transaction
from guillotina_elasticsearch.events import IIndexProgress
from guillotina_elasticsearch.events import IndexProgress
from guillotina_elasticsearch.interfaces import DOC_TYPE
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.migration import Migrator
from guillotina_elasticsearch.reindex import Reindexer
from guillotina_elasticsearch.tests.utils import add_content
from guillotina_elasticsearch.tests.utils import run_with_retries
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import aioelasticsearch
import aiotask_context
import asyncio
import json
import pytest
import random


@pytest.mark.flaky(reruns=5)
async def _test_migrate_while_content_getting_added(es_requester):
    async with es_requester as requester:
        add_count = await add_content(requester)

        container, request, txn, tm = await setup_txn_on_container(requester)

        search = get_utility(ICatalogUtility)
        await search.refresh(container)
        await asyncio.sleep(3)

        assert add_count == await search.get_doc_count(container)

        migrator = Migrator(search, container, force=True)
        add_content_task1 = asyncio.ensure_future(add_content(requester, base_id='foo1-'))
        add_content_task2 = asyncio.ensure_future(add_content(requester, base_id='foo2-'))
        reindex_task = asyncio.ensure_future(migrator.run_migration())

        await asyncio.wait([add_content_task1, reindex_task, add_content_task2])
        await search.refresh(container)
        await asyncio.sleep(3)

        idx_count = await search.get_doc_count(container)
        # +1 here because container ob now indexed and it isn't by default in tests
        assert (add_count * 3) + 1 == idx_count

        await tm.abort(txn=txn)


async def test_migrate_get_all_uids(es_requester):
    async with es_requester as requester:
        await add_content(requester)

        container, request, txn, tm = await setup_txn_on_container(requester)

        search = get_utility(ICatalogUtility)
        await asyncio.sleep(1)
        await search.refresh(container)
        await asyncio.sleep(1)

        current_count = await search.get_doc_count(container)

        migrator = Migrator(search, container, force=True)
        uids = await migrator.get_all_uids()

        assert len(uids) == current_count

        await tm.abort(txn=txn)


@pytest.mark.flaky(reruns=5)
async def test_removes_orphans(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        await search.index(container, {
            'foobar': {
                'title': 'foobar',
                'type_name': 'Item'
            }
        })
        # foobar here is an orphaned object because it doesn't reference an object

        im = get_adapter(container, IIndexManager)
        index_name = await im.get_index_name()  # alias

        doc = await search.conn.get(
            index=index_name, doc_type=DOC_TYPE, id='foobar')
        assert doc['found']

        migrator = Migrator(search, container, force=True)
        await migrator.run_migration()

        async def _test():
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await search.conn.get(
                    index=index_name, doc_type=DOC_TYPE, id='foobar')

            assert len(migrator.orphaned) == 1
            assert migrator.orphaned[0] == 'foobar'

        await run_with_retries(_test, requester)


# @pytest.mark.flaky(reruns=5)
async def test_fixes_missing(es_requester):
    async with es_requester as requester:
        await add_content(requester, 2, 2)
        container, request, txn, tm = await setup_txn_on_container(requester)

        search = get_utility(ICatalogUtility)
        await asyncio.sleep(1)
        await search.refresh(container)
        await asyncio.sleep(1)
        original_count = await search.get_doc_count(container)

        keys = await container.async_keys()
        key = random.choice(keys)
        ob = await container.async_get(key)
        await search.remove(container, [(ob)], request=request)

        await asyncio.sleep(1)
        await search.refresh(container)
        await asyncio.sleep(1)
        assert original_count != await search.get_doc_count(container)
        im = get_adapter(container, IIndexManager)
        old_index_name = await im.get_real_index_name()

        responses = []
        class Writer:
            def write(self, item):
                responses.append(item)

        migrator = Migrator(search, container, force=True,
                            request=request, response=Writer())
        await migrator.run_migration()

        assert migrator.status == 'done'

        await asyncio.sleep(1)
        await search.refresh(container)
        await asyncio.sleep(1)
        # new index should fix missing one, old index still has it missing
        num_docs = await search.get_doc_count(container, migrator.work_index_name)
        assert num_docs == original_count
        assert old_index_name != await im.get_real_index_name()


@pytest.mark.flaky(reruns=5)
async def test_updates_index_data(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)

        migrator = Migrator(search, container, force=True, request=request)
        new_index_name = await migrator.create_next_index()
        migrator.work_index_name = new_index_name
        index_manager = get_adapter(container, IIndexManager)

        ob = create_content()
        ob.title = 'foobar'
        await migrator.index_object(ob, full=True)
        assert len(migrator.batch) == 1
        assert [v for v in migrator.batch.values()][0]['action'] == 'index'

        await migrator.flush()
        assert len(migrator.batch) == 0
        await migrator.join_futures()
        await asyncio.sleep(1)
        await search.refresh(container, new_index_name)
        await asyncio.sleep(1)
        assert await search.get_doc_count(container, new_index_name) == 1

        # test updating doc
        migrator.mapping_diff = {
            'title': {}
        }
        ob.title = 'foobar-new'
        await migrator.index_object(ob, full=False)
        assert len(migrator.batch) == 1
        assert [v for v in migrator.batch.values()][0]['action'] == 'update'

        assert len([v for v in migrator.batch.values()][0]['data']) == 2
        assert [v for v in migrator.batch.values()][0]['data']['title'] == 'foobar-new'

        await migrator.flush()
        assert len(migrator.batch) == 0
        await migrator.join_futures()
        await asyncio.sleep(1)
        await search.refresh(container, new_index_name)
        await asyncio.sleep(1)
        doc = await search.conn.get(
            index=new_index_name, doc_type=DOC_TYPE, id=ob._p_oid)
        assert doc['_source']['title'] == 'foobar-new'


async def test_calculate_mapping_diff(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)

        index_manager = get_adapter(container, IIndexManager)
        migrator = Migrator(search, container, force=True, request=request)
        new_index_name = await index_manager.start_migration()
        migrator.work_index_name = new_index_name

        mappings = await index_manager.get_mappings()

        # tweak mappings so we can get the diff...
        if 'creators' in mappings['properties']:
            mappings['properties']['creators']['type'] = 'text'
        mappings['properties']['foobar'] = {'type': 'keyword', 'index': True}

        await search.create_index(
            new_index_name, index_manager, mappings=mappings)

        diff = await migrator.calculate_mapping_diff()
        assert len(diff) == 2


async def test_updates_index_name(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        im = get_adapter(container, IIndexManager)
        existing_index = await im.get_real_index_name()
        assert await search.conn.indices.exists(existing_index)
        migrator = Migrator(search, container, force=True, request=request)
        await migrator.run_migration()
        assert not await search.conn.indices.exists(existing_index)
        assert await search.conn.indices.exists(migrator.work_index_name)
        assert await im.get_real_index_name() == migrator.work_index_name


async def test_moves_docs_over(es_requester):
    async with es_requester as requester:
        await add_content(requester)
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)

        await asyncio.sleep(1)
        await search.refresh(container)
        await asyncio.sleep(1)
        current_count = await search.get_doc_count(container)

        migrator = Migrator(search, container, force=True, request=request)
        await migrator.run_migration()

        im = get_adapter(container, IIndexManager)
        assert await im.get_real_index_name() == migrator.work_index_name
        await asyncio.sleep(1)
        await search.refresh(container)
        await asyncio.sleep(1)
        assert await search.get_doc_count(container) == current_count


async def test_create_next_index(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        migrator = Migrator(search, container, force=True, request=request)
        name = await migrator.create_next_index()
        assert name == 'guillotina-db-guillotina_2'


async def test_unindex_during_next_index(es_requester):
    async with es_requester as requester:
        await add_content(requester, 2)
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        index_manager = get_adapter(container, IIndexManager)
        work_index_name = await index_manager.start_migration()
        await search.create_index(work_index_name, index_manager)
        await tm.commit(txn=txn)
        container, request, txn, tm = await setup_txn_on_container(requester)
        keys = await container.async_keys()
        item = await container.async_get(keys[0])
        aiotask_context.set('request', request)
        await notify(ObjectRemovedEvent(item, container, item.id))
        request.execute_futures()
        await asyncio.sleep(1)


class FakeEventHandler:
    called = None

    async def subscribe(self, event):
        self.called = True
        if not getattr(self, "event", None):
            self.event = []
        self.event.append(event)


@pytest.fixture(scope='function')
def event_handler():
    return FakeEventHandler()


async def test_migrator_emit_events_during_indexing(es_requester, event_handler):
    async with es_requester as requester:
        container, req, txn, tm = await setup_txn_on_container(requester)  # pylint: disable=W0612
        search = get_utility(ICatalogUtility)

        _marker = {}
        gr.base.adapters.subscribe([IIndexProgress], None, event_handler.subscribe)
        migrator = Reindexer(
            search, _marker, force=True, request=req, reindex_security=True
        )
        migrator.bulk_size = 0
        migrator.batch = {}
        migrator.existing = {}
        migrator.processed = 1
        migrator.missing = {'xx': 1}
        await migrator.attempt_flush()
        assert event_handler.called == True
        assert isinstance(event_handler.event[0], IndexProgress)
        assert event_handler.event[0].context == _marker


async def test_migrator_emmits_events_on_end(es_requester, event_handler):
    async with es_requester as requester:
        resp, status = await requester(
            'POST',
            '/db/guillotina/',
            data=json.dumps({
                '@type': 'Folder',
                'title': 'Folder',
                'id': 'foobar'
            })
        )

        container, req, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)

        gr.base.adapters.subscribe([IIndexProgress], None, event_handler.subscribe)
        migrator = Reindexer(
            search, container, force=True, request=req, reindex_security=True
        )

        ob = await container.async_get('foobar')
        await migrator.reindex(ob)
        assert event_handler.called == True
        assert len(event_handler.event) == 2
        assert event_handler.event[0].completed == None
        assert event_handler.event[0].processed == 0
        assert event_handler.event[1].completed == True
        assert event_handler.event[0].context == container


async def test_search_works_on_new_docs_during_migration(es_requester):
    async with es_requester as requester:
        await add_content(requester, 2)
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        migrator = Migrator(search, container, force=True, request=request)
        im = get_adapter(container, IIndexManager)
        index_name = await im.get_index_name()
        next_index_name = await migrator.setup_next_index()

        resp, _ = await requester('POST', '/db/guillotina', data=json.dumps({
            '@type': 'Item'
        }))

        async def _test():
            result1 = await search.conn.get(
                index=next_index_name, doc_type='_all', id=resp['@uid'])
            assert result1 is not None
            result2 = await search.conn.get(
                index=index_name, doc_type='_all', id=resp['@uid'])
            assert result2 is not None

        await run_with_retries(_test, requester)


async def test_search_works_on_updated_docs_during_migration_when_missing(es_requester):
    '''
    - started migration
    - doc update
    - doc missing in next index during update
    '''
    async with es_requester as requester:
        resp, _ = await requester('POST', '/db/guillotina', data=json.dumps({
            '@type': 'Item',
            'title': 'Foobar1',
            'id': 'foobar'
        }))

        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        migrator = Migrator(search, container, force=True, request=request)
        im = get_adapter(container, IIndexManager)
        index_name = await im.get_index_name()
        next_index_name = await migrator.setup_next_index()

        await requester('PATCH', '/db/guillotina/foobar', data=json.dumps({
            'title': 'Foobar2'
        }))

        async def _test():
            result1 = await search.conn.get(
                index=index_name, doc_type='_all', id=resp['@uid'])
            assert result1 is not None
            assert result1['_source']['title'] == 'Foobar2'
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await search.conn.get(
                    index=next_index_name, doc_type='_all', id=resp['@uid'])

        await run_with_retries(_test, requester)


async def test_search_works_on_updated_docs_during_migration_when_present(es_requester):
    '''
    - started migration
    - doc update
    - doc also updated in next index
    '''
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        migrator = Migrator(search, container, force=True, request=request)
        im = get_adapter(container, IIndexManager)
        index_name = await im.get_index_name()
        next_index_name = await migrator.setup_next_index()

        resp, _ = await requester('POST', '/db/guillotina', data=json.dumps({
            '@type': 'Item',
            'title': 'Foobar1',
            'id': 'foobar'
        }))
        await asyncio.sleep(1)
        await requester('PATCH', '/db/guillotina/foobar', data=json.dumps({
            'title': 'Foobar2'
        }))

        async def _test():
            result1 = await search.conn.get(
                index=next_index_name, doc_type='_all', id=resp['@uid'])
            assert result1 is not None
            assert result1['_source']['title'] == 'Foobar2'
            result2 = await search.conn.get(
                index=index_name, doc_type='_all', id=resp['@uid'])
            assert result2 is not None
            assert result2['_source']['title'] == 'Foobar2'

        await run_with_retries(_test, requester)


@pytest.mark.flaky(reruns=5)
async def test_delete_in_both_during_migration(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        migrator = Migrator(search, container, force=True, request=request)
        im = get_adapter(container, IIndexManager)
        index_name = await im.get_index_name()
        next_index_name = await migrator.setup_next_index()

        resp, _ = await requester('POST', '/db/guillotina', data=json.dumps({
            '@type': 'Folder',
            'title': 'Foobar1',
            'id': 'foobar'
        }))
        await requester('DELETE', '/db/guillotina/foobar')

        async def _test():
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await search.conn.get(
                    index=next_index_name, doc_type='_all', id=resp['@uid'])
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await search.conn.get(
                    index=index_name, doc_type='_all', id=resp['@uid'])

        await run_with_retries(_test, requester)



async def test_migrate_content_index_works(es_requester):
    async with es_requester as requester:
        add_count = await add_content(requester)
        cresp, _ = await requester(
            'POST',
            '/db/guillotina/',
            data=json.dumps({
                '@type': 'UniqueIndexContent',
                'id': 'foobar'
            })
        )
        await requester(
            'POST',
            '/db/guillotina/foobar',
            data=json.dumps({
                '@type': 'IndexItemContent'
            })
        )

        container, request, txn, tm = await setup_txn_on_container(requester)

        search = get_utility(ICatalogUtility)
        await search.refresh(container)
        await asyncio.sleep(3)

        assert (add_count + 1) == await search.get_doc_count(container, 'guillotina-db-guillotina_1')
        assert await search.get_doc_count(
            container, '1_guillotina-db-guillotina__uniqueindexcontent-{}'.format(
                get_short_oid(cresp['@uid'])
            )) == 1

        migrator = Migrator(search, container, force=True)
        await migrator.run_migration()

        assert await search.conn.indices.exists('guillotina-db-guillotina_2')
        assert not await search.conn.indices.exists('guillotina-db-guillotina_1')
        assert await search.conn.indices.exists(
            '2_guillotina-db-guillotina__uniqueindexcontent-{}'.format(
                get_short_oid(cresp['@uid'])
            ))
        assert not await search.conn.indices.exists(
            '1_guillotina-db-guillotina__uniqueindexcontent-{}'.format(
                get_short_oid(cresp['@uid'])
            ))

        assert (add_count + 1) == await search.get_doc_count(container, 'guillotina-db-guillotina_2')
        assert await search.get_doc_count(
            container, '2_guillotina-db-guillotina__uniqueindexcontent-{}'.format(
                get_short_oid(cresp['@uid'])
            )) == 1
