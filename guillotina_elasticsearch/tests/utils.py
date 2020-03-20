from aioelasticsearch import Elasticsearch
from guillotina import app_settings
from guillotina import task_vars
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.tests import utils
from guillotina.tests.utils import get_container
from typing import Optional

import aioelasticsearch.exceptions
import asyncio
import elasticsearch.exceptions
import json
import time


async def add_content(
    requester, num_folders=10, num_items=10, base_id="es-", path="/db/guillotina/"
):
    created = 0
    for fidx in range(num_folders):
        folder_id = f"{base_id}folder{str(fidx)}"
        resp, status = await requester(
            "POST",
            path,
            data=json.dumps(
                {"@type": "Folder", "title": "Folder" + str(fidx), "id": folder_id}
            ),
        )
        created += 1
        assert status == 201
        path += "/" + folder_id
        for idx in range(num_items):
            resp, status = await requester(
                "POST",
                path,
                data=json.dumps({"@type": "Example", "title": "Item" + str(idx)}),
            )
            created += 1
            assert status == 201
    return created


async def setup_txn_on_container(requester, container_id="guillotina"):
    utils.login()
    request = utils.get_mocked_request(db=requester.db)
    task_vars.request.set(request)
    container = await get_container(requester=requester, container_id=container_id)
    tm = task_vars.tm.get()
    txn = await tm.begin()

    return container, request, txn, tm


async def run_with_retries(func, requester=None, timeout=10, retry_wait=0.5):
    start = time.time()
    exception = None
    times = 0

    from _pytest.outcomes import Failed

    while (time.time() - start) < timeout:
        try:
            times += 1
            return await func()
        except (
            AssertionError,
            KeyError,
            aioelasticsearch.exceptions.NotFoundError,
            aioelasticsearch.exceptions.TransportError,
            Failed,
        ) as ex:
            exception = ex
            await asyncio.sleep(retry_wait)
            if requester is not None:
                search = get_utility(ICatalogUtility)
                await search.refresh(index_name="")
    print(f"failed after trying {times} times")
    if exception is not None:
        raise exception  # pylint: disable=E0702
    else:
        raise AssertionError("unknown assertion error")


async def cleanup_es(
    prefix: Optional[str] = None, delete: bool = True, close: bool = False
):
    conn = Elasticsearch(**app_settings["elasticsearch"]["connection_settings"])

    for alias in (await conn.cat.aliases()).splitlines():
        name, index = alias.split()[:2]

        if name[0] == "." or index[0] == ".":
            # ignore indexes that start with .
            continue

        if prefix and name.startswith(prefix) or not prefix:
            await _delete_alias(conn, index, name)
            if close:
                await _close_index(conn, index)
            if delete:
                await _delete_index(conn, index)

    for index in (await conn.cat.indices()).splitlines():
        _, _, index_name = index.split()[:3]
        if index_name[0] == ".":
            # ignore indexes that start with .
            continue

        if prefix and index_name.startswith(prefix) or not prefix:
            if close:
                await _close_index(conn, index)
            if delete:
                await _delete_index(conn, index)


async def _delete_alias(conn, index_name, alias_name):
    try:
        await conn.indices.delete_alias(index_name, alias_name)
    except (
        elasticsearch.exceptions.AuthorizationException,
        elasticsearch.exceptions.NotFoundError,
    ):
        pass


async def _delete_index(conn, index_name):
    try:
        await conn.indices.delete(index_name)
    except elasticsearch.exceptions.AuthorizationException:
        pass
    except elasticsearch.exceptions.NotFoundError:
        # Index not found
        pass


async def _close_index(conn, index_name):
    try:
        await conn.indices.close(index_name)
    except elasticsearch.exceptions.NotFoundError:
        # Index not found, already deleted
        pass
    except elasticsearch.exceptions.RequestError as ex:
        if ex.error != "index_closed_exception":
            # index closed exception is ignored here.
            raise
        # Index already closed
        pass
