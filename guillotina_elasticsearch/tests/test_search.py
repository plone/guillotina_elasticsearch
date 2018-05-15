from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina_elasticsearch.tests.utils import run_with_retries
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import asyncio
import json


async def test_indexing_and_search(es_requester):
    async with es_requester as requester:
        resp, status = await requester(
            'POST',
            '/db/guillotina/',
            data=json.dumps({
                '@type': 'Example',
                'title': 'Item1',
                'id': 'item1',
                'categories': [{
                    'label': 'term1',
                    'number': 1.0
                }, {
                    'label': 'term2',
                    'number': 2.0
                }]
            })
        )
        assert status == 201

        async def _test():
            resp, status = await requester(
                'POST',
                '/db/guillotina/@search',
                data=json.dumps({})
            )
            assert resp['items_count'] == 1
            assert resp['member'][0]['path'] == '/item1'

        await run_with_retries(_test, requester)

        # try removing now...
        await requester('DELETE', '/db/guillotina/item1')

        async def _test():
            resp, status = await requester(
                'POST',
                '/db/guillotina/@search',
                data=json.dumps({})
            )
            assert resp['items_count'] == 0

        await run_with_retries(_test, requester)


async def test_removes_all_children(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # pylint: disable=W0612
        search = get_utility(ICatalogUtility)

        resp, status = await requester(
            'POST', '/db/guillotina/',
            data=json.dumps({
                '@type': 'Folder',
                'title': 'Folder1',
                'id': 'folder1'
            })
        )
        assert status == 201
        resp, status = await requester(
            'POST', '/db/guillotina/folder1',
            data=json.dumps({
                '@type': 'Folder',
                'title': 'Folder2',
                'id': 'folder2'
            })
        )
        assert status == 201
        resp, status = await requester(
            'POST', '/db/guillotina/folder1/folder2',
            data=json.dumps({
                '@type': 'Folder',
                'title': 'Folder3',
                'id': 'folder3'
            })
        )
        assert status == 201

        async def _test():
            resp, status = await requester(
                'POST',
                '/db/guillotina/@search',
                data=json.dumps({})
            )
            assert resp['items_count'] == 3
            assert resp['member'][0]['@name']

        await run_with_retries(_test, requester)

        # try removing now...
        await requester('DELETE', '/db/guillotina/folder1')

        async def _test():
            resp, status = await requester(
                'POST',
                '/db/guillotina/@search',
                data=json.dumps({})
            )
            assert resp['items_count'] == 0

        await run_with_retries(_test, requester)
