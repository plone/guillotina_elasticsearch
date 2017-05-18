from guillotina.component import getUtility
from guillotina.interfaces import ICatalogUtility
from guillotina_elasticsearch.tests.utils import setup_txn_on_container


async def test_index(es_requester):
    async with await es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = getUtility(ICatalogUtility)
        current_count = await search.get_doc_count(container)
        await search.index(container, {
            'foobar': {
                'title': 'foobar',
                'type_name': 'Item'
            }
        })
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count + 1


async def test_update(es_requester):
    async with await es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = getUtility(ICatalogUtility)
        current_count = await search.get_doc_count(container)
        await search.index(container, {
            'foobar': {
                'title': 'foobar',
                'type_name': 'Item'
            }
        })
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count + 1
        await search.update(container, {
            'foobar': {
                'title': 'foobar-updated',
                'type_name': 'Item'
            }
        })
        await search.refresh(container)
        doc = await search.conn.get(await search.get_index_name(container), 'foobar')
        assert doc['_source']['title'] == 'foobar-updated'


async def test_delete(es_requester):
    async with await es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = getUtility(ICatalogUtility)
        current_count = await search.get_doc_count(container)
        await search.index(container, {
            'foobar': {
                'title': 'foobar',
                'type_name': 'Item'
            }
        })
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count + 1
        await search.remove(container, [('foobar', 'Item', '/foobar')], future=False)
        await search.refresh(container)
        assert await search.get_doc_count(container) == current_count
