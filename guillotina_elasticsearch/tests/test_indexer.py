from guillotina_elasticsearch.migration import Indexer
from guillotina_elasticsearch.tests.utils import setup_txn_on_container
from guillotina.interfaces import ICatalogDataAdapter
import json


async def test_indexer_matches_manual(es_requester):
    async with await es_requester as requester:
        resp, status = await requester(
            'POST',
            '/db/guillotina/',
            data=json.dumps({
                '@type': 'Folder',
                'title': 'Folder',
                'id': 'foobar'
            })
        )
        container, request, txn, tm = await setup_txn_on_container(requester)
        ob = await container.async_get('foobar')
        full_data = await ICatalogDataAdapter(ob)()
        indexer = Indexer()
        for key, value in full_data.items():
            assert value == await indexer.get_value(ob, key)
