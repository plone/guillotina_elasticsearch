import json
import time
from guillotina_elasticsearch.reindex import Reindexer
from guillotina.interfaces import ICatalogUtility
from guillotina.component import getUtility
from guillotina.tests import utils


async def test_indexing_and_search(es_requester):
    async with await es_requester as requester:
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
        time.sleep(1)
        resp, status = await requester(
            'POST',
            '/db/guillotina/@search',
            data=json.dumps({})
        )
        assert resp['items_count'] == 1
        assert resp['member'][0]['path'] == '/item1'

        # try removing now...
        await requester('DELETE', '/db/guillotina/item1')
        time.sleep(1)

        resp, status = await requester(
            'POST',
            '/db/guillotina/@search',
            data=json.dumps({})
        )
        assert resp['items_count'] == 0


async def test_reindex(es_requester):
    async with await es_requester as requester:
        resp, status = await requester(
            'POST',
            '/db/guillotina/',
            data=json.dumps({
                '@type': 'Example',
                'title': 'Item1',
                'id': 'item1'
            })
        )
        resp, status = await requester(
            'POST',
            '/db/guillotina/',
            data=json.dumps({
                '@type': 'Example',
                'title': 'Item2',
                'id': 'item2'
            })
        )
        resp, status = await requester(
            'POST',
            '/db/guillotina/',
            data=json.dumps({
                '@type': 'Example',
                'title': 'Item3',
                'id': 'item3'
            })
        )
        assert status == 201
        time.sleep(1)

        request = utils.get_mocked_request(requester.db)
        root = await utils.get_root(request)
        txn = await request._tm.begin(request)
        container = await root.async_get('guillotina')
        request.container = container

        search = getUtility(ICatalogUtility)
        reindexer = Reindexer(search, container)
        await reindexer.all_content()

        reindexer = Reindexer(search, container, clean=True)
        await reindexer.all_content()

        await request._tm.abort(txn=txn)
