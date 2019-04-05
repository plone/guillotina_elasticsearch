from aioelasticsearch import Elasticsearch
from guillotina import app_settings
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.tests import utils
from guillotina.transactions import managed_transaction

import aioelasticsearch.exceptions
import asyncio
import elasticsearch.exceptions
import json
import time


async def add_content(requester, num_folders=10, num_items=10, base_id='es-',
                      path='/db/guillotina/'):
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
    return created


async def setup_txn_on_container(requester, container_id='guillotina'):
    request = utils.get_mocked_request(requester.db)
    utils.login(request)
    container = await get_container(request=request, container_id=container_id)
    request.container = container

    tm = request._tm
    txn = await tm.begin(request)
    return container, request, txn, tm


async def get_container(requester=None, request=None,
                        container_id='guillotina'):
    if request is None:
        request = utils.get_mocked_request(requester.db)
    root = await utils.get_root(request)
    async with managed_transaction(request=request):
        container = await root.async_get(container_id)
        request._container_id = container.id
        request.container = container
        return container


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
                search = get_utility(ICatalogUtility)
                await search.refresh(index_name='')
    print(f'failed after trying {times} times')
    if exception is not None:
        raise exception  # pylint: disable=E0702
    else:
        raise AssertionError("unknown assertion error")


async def cleanup_es(es_host, prefix=''):
    conn = Elasticsearch(
        **app_settings['elasticsearch']["connection_settings"])
    for alias in (await conn.cat.aliases()).splitlines():
        name, index = alias.split()[:2]
        if name[0] == '.' or index[0] == '.':
            # ignore indexes that start with .
            continue
        if name.startswith(prefix):
            try:
                await conn.indices.delete_alias(index, name)
                await conn.indices.delete(index)
            except elasticsearch.exceptions.AuthorizationException:
                pass
    for index in (await conn.cat.indices()).splitlines():
        _, _, index_name = index.split()[:3]
        if index_name[0] == '.':
            # ignore indexes that start with .
            continue
        if index_name.startswith(prefix):
            try:
                await conn.indices.delete(index_name)
            except elasticsearch.exceptions.AuthorizationException:
                pass
