from aioelasticsearch import Elasticsearch
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.tests import utils
from guillotina.tests.utils import get_container
from guillotina.tests.utils import get_mocked_request
from guillotina.transactions import managed_transaction

import aioelasticsearch.exceptions
import asyncio
import json
import time


async def add_content(requester, num_folders=10, num_items=10, base_id='es-'):
    path = '/db/guillotina/'
    created = 0
    for fidx in range(num_folders):
        folder_id = f'{base_id}folder{str(fidx)}'
        resp, status = await requester(
            'POST',
            path,
            data=json.dumps({
                '@type': 'Folder',
                'title': 'Folder' + str(fidx),
                'id': folder_id
            })
        )
        created += 1
        assert status == 201
        path += '/' + folder_id
        for idx in range(num_items):
            resp, status = await requester(
                'POST',
                path,
                data=json.dumps({
                    '@type': 'Example',
                    'title': 'Item' + str(idx)
                })
            )
            created += 1
            assert status == 201
    await asyncio.sleep(1)  # make sure async index tasks finish
    return created


async def setup_txn_on_container(requester):
    request = utils.get_mocked_request(requester.db)
    utils.login(request)
    container = await utils.get_container(request=request)
    request.container = container

    tm = request._tm
    txn = await tm.begin(request)
    return container, request, txn, tm


async def refresh_index(requester):
    search = get_utility(ICatalogUtility)
    request = get_mocked_request(requester.db)
    container = await get_container(request=request)
    async with managed_transaction(
            request=request, adopt_parent_txn=True, abort_when_done=True):
        await search.refresh(container)


async def run_with_retries(func, requester=None, timeout=10, retry_wait=0.5):
    start = time.time()
    exception = None
    times = 0

    from _pytest.outcomes import Failed

    while (time.time() - start) < timeout:
        try:
            times += 1
            return await func()
        except (AssertionError, KeyError,
                aioelasticsearch.exceptions.NotFoundError,
                aioelasticsearch.exceptions.TransportError,
                Failed) as ex:
            exception = ex
            await asyncio.sleep(retry_wait)
            if requester is not None:
                await refresh_index(requester)
    print(f'failed after trying {times} times')
    if exception is not None:
        raise exception  # pylint: disable=E0702
    else:
        raise AssertionError("unknown assertion error")


async def cleanup_es(es_host):
    conn = Elasticsearch(hosts=[es_host])
    for alias in (await conn.cat.aliases()).splitlines():
        name, index = alias.split()[:2]
        await conn.indices.delete_alias(index, name)
    for index in (await conn.cat.indices()).splitlines():
        _, _, index_name = index.split()[:3]
        await conn.indices.delete(index_name)
