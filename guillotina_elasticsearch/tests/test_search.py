import json
import time


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
