from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.utils import get_content_path
from guillotina_elasticsearch.commands.vacuum import Vacuum
from guillotina_elasticsearch.tests.utils import add_content
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import asyncio
import os
import pytest
import random, json


DATABASE = os.environ.get('DATABASE', 'DUMMY')


@pytest.mark.skipif(DATABASE == 'DUMMY', reason='Not for dummy db')
async def test_adds_missing_elasticsearch_entry(es_requester):
    async with es_requester as requester:
        await add_content(requester)
        await asyncio.sleep(1)

        container, request, txn, tm = await setup_txn_on_container(requester)

        keys = await container.async_keys()
        key = random.choice(keys)
        ob = await container.async_get(key)
        search = get_utility(ICatalogUtility)
        await search.remove(container, [(
            ob._p_oid, ob.type_name, get_content_path(ob)
        )], request=request)

        await asyncio.sleep(1)

        vacuum = Vacuum(txn, tm, request, container)
        await vacuum.setup()
        await vacuum.check_orphans()
        await vacuum.check_missing()

        assert len(vacuum.missing) > 0

        await tm.abort(txn=txn)


@pytest.mark.skipif(DATABASE == 'DUMMY', reason='Not for dummy db')
async def test_removes_orphaned_es_entry(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        index_name = await search.get_index_name(container)
        await search.index(container, {
            'foobar': {
                'title': 'foobar',
                'type_name': 'Item'
            }
        })
        await search.refresh(container, index_name)
        await asyncio.sleep(1)

        vacuum = Vacuum(txn, tm, request, container)
        await vacuum.setup()
        await vacuum.check_orphans()
        await vacuum.check_missing()

        assert len(vacuum.orphaned) == 1

        await tm.abort(txn=txn)


@pytest.mark.skipif(DATABASE == 'DUMMY', reason='Not for dummy db')
async def test_reindexes_moved_content(es_requester):
    async with es_requester as requester:
        resp1, _ = await requester(
            'POST',
            '/db/guillotina/',
            data=json.dumps({
                '@type': 'Folder',
                'id': 'foobar'
            })
        )
        resp2, _ = await requester(
            'POST',
            '/db/guillotina/foobar',
            data=json.dumps({
                '@type': 'Folder',
                'id': 'foobar'
            })
        )
        resp3, _ = await requester(
            'POST',
            '/db/guillotina/foobar/foobar',
            data=json.dumps({
                '@type': 'Folder',
                'id': 'foobar'
            })
        )

        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        index_name = await search.get_index_name(container)

        await asyncio.sleep(2)
        assert await search.get_doc_count(container) == 3
        result = await search.conn.get(
            index=index_name, doc_type='_all', id=resp3['@uid'])
        assert result is not None

        # mess with index data to make it look like it was moved
        await search.conn.update(
            index=index_name,
            id=resp1['@uid'],
            doc_type='Folder',
            body={
                "doc": {
                    "path": "/moved-foobar",
                    "parent_uuid": "FOOOBBAR MOVED TO NEW PARENT"
                }
            })
        await search.conn.update(
            index=index_name,
            id=resp2['@uid'],
            doc_type='Folder',
            body={
                "doc": {
                    "path": "/moved-foobar/foobar"
                }
            })
        await search.conn.update(
            index=index_name,
            id=resp3['@uid'],
            doc_type='Folder',
            body={
                "doc": {
                    "path": "/moved-foobar/foobar/foobar"
                }
            })

        await asyncio.sleep(2)

        result = await search.conn.get(
            index=index_name, doc_type='_all', id=resp3['@uid'])
        assert result['_source']['path'] == "/moved-foobar/foobar/foobar"
        result = await search.conn.get(
            index=index_name, doc_type='_all', id=resp1['@uid'])
        assert result['_source']['path'] == "/moved-foobar"
        assert result['_source']['parent_uuid'] == "FOOOBBAR MOVED TO NEW PARENT"

        vacuum = Vacuum(txn, tm, request, container)
        await vacuum.setup()
        await vacuum.check_missing()

        assert len(vacuum.orphaned) == 0
        assert len(vacuum.missing) == 1

        await asyncio.sleep(2)

        result = await search.conn.get(
            index=index_name, doc_type='_all', id=resp3['@uid'])
        assert result['_source']['path'] == "/foobar/foobar/foobar"
        result = await search.conn.get(
            index=index_name, doc_type='_all', id=resp1['@uid'])
        assert result['_source']['path'] == "/foobar"
        assert result['_source']['parent_uuid'] != "FOOOBBAR MOVED TO NEW PARENT"

        await tm.abort(txn=txn)
