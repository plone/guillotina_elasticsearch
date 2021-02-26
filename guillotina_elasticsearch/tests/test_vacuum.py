from guillotina import task_vars
from guillotina.component import get_utility
from guillotina.db.uid import get_short_uid
from guillotina.interfaces import ICatalogUtility
from guillotina_elasticsearch.commands.vacuum import Vacuum
from guillotina_elasticsearch.interfaces import DOC_TYPE
from guillotina_elasticsearch.tests.utils import add_content
from guillotina_elasticsearch.tests.utils import run_with_retries
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import asyncio
import json
import os
import pytest


pytestmark = [pytest.mark.asyncio]


DATABASE = os.environ.get("DATABASE", "DUMMY")


@pytest.mark.skipif(DATABASE == "DUMMY", reason="Not for dummy db")
async def test_adds_missing_elasticsearch_entry(es_requester):
    async with es_requester as requester:
        await add_content(requester)

        search = get_utility(ICatalogUtility)
        container, request, txn, tm = await setup_txn_on_container(requester)
        task_vars.request.set(request)

        async def _test():
            assert await search.get_doc_count(container) == 110

        await run_with_retries(_test, requester)

        for key in await container.async_keys():
            ob = await container.async_get(key)
            await search.remove(container, [ob], request=request)

        async def __test():
            assert await search.get_doc_count(container) == 0

        await run_with_retries(__test, requester)

        vacuum = Vacuum(txn, tm, container)
        await vacuum.setup()
        await vacuum.check_missing()
        await vacuum.check_orphans()

        assert len(vacuum.orphaned) == 0
        assert len(vacuum.out_of_date) == 0
        assert len(vacuum.missing) == 110

        async def ___test():
            assert await search.get_doc_count(container) == 110

        await run_with_retries(___test, requester)

        await tm.abort(txn=txn)


@pytest.mark.skipif(DATABASE == "DUMMY", reason="Not for dummy db")
@pytest.mark.flaky(reruns=5)
async def test_updates_out_of_data_es_entries(es_requester):
    async with es_requester as requester:
        await add_content(requester)
        await asyncio.sleep(1)

        container, request, txn, tm = await setup_txn_on_container(requester)
        task_vars.request.set(request)

        search = get_utility(ICatalogUtility)
        index_name = await search.get_container_index_name(container)
        await search.update_by_query(
            {"script": {"lang": "painless", "inline": "ctx._source.tid = 0"}},
            indexes=[index_name],
        )

        async def _test():
            assert await search.get_doc_count(container) == 110

        await run_with_retries(_test, requester)

        await asyncio.sleep(1)

        vacuum = Vacuum(txn, tm, container)
        await vacuum.setup()
        await vacuum.check_missing()
        await vacuum.check_orphans()

        assert len(vacuum.orphaned) == 0
        assert len(vacuum.missing) == 0
        assert len(vacuum.out_of_date) == 110

        await tm.abort(txn=txn)


@pytest.mark.skipif(DATABASE == "DUMMY", reason="Not for dummy db")
async def test_removes_orphaned_es_entry(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        await search.index(
            container, {"foobar": {"title": "foobar", "type_name": "Item"}}
        )

        async def _test():
            assert await search.get_doc_count(container) == 1

        await run_with_retries(_test, requester)

        vacuum = Vacuum(txn, tm, container)
        await vacuum.setup()
        await vacuum.check_orphans()
        await vacuum.check_missing()

        assert len(vacuum.orphaned) == 1
        assert len(vacuum.missing) == 0
        assert len(vacuum.out_of_date) == 0

        async def __test():
            assert await search.get_doc_count(container) == 0

        await run_with_retries(__test, requester)

        await tm.abort(txn=txn)


@pytest.mark.skipif(DATABASE == "DUMMY", reason="Not for dummy db")
async def test_vacuum_with_sub_indexes(es_requester):
    async with es_requester as requester:
        await add_content(requester, num_folders=2, num_items=5, path="/db/guillotina/")

        cresp, _ = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "UniqueIndexContent",
                    "title": "UniqueIndexContent",
                    "id": "foobar",
                }
            ),
        )
        await add_content(
            requester, num_folders=2, num_items=5, path="/db/guillotina/foobar"
        )  # noqa

        search = get_utility(ICatalogUtility)
        content_index_name = (
            "guillotina-db-guillotina__uniqueindexcontent-{}".format(  # noqa
                get_short_uid(cresp["@uid"])
            )
        )
        container, request, txn, tm = await setup_txn_on_container(requester)
        task_vars.request.set(request)

        await asyncio.sleep(1)

        async def _test():
            assert await search.get_doc_count(container) == 13
            assert (
                await search.get_doc_count(index_name=content_index_name) == 12
            )  # noqa

        await run_with_retries(_test, requester)

        for key in await container.async_keys():
            if key == "foobar":
                continue
            ob = await container.async_get(key)
            await search.remove(container, [ob], request=request)

        await asyncio.sleep(1)

        foobar = await container.async_get("foobar")
        for key in await foobar.async_keys():
            ob = await foobar.async_get(key)
            await search.remove(container, [ob], request=request)

        await asyncio.sleep(1)

        await search.index(
            container, {"foobar1": {"title": "foobar", "type_name": "Item"}}
        )
        await search.index(
            container,
            {
                "foobar2": {
                    "title": "foobar",
                    "type_name": "Item",
                    "__indexes__": [content_index_name],
                }
            },
        )

        async def __test():
            assert await search.get_doc_count(container) == 2
            assert (
                await search.get_doc_count(index_name=content_index_name) == 1
            )  # noqa

        await run_with_retries(__test, requester)

        vacuum = Vacuum(txn, tm, container)
        await vacuum.setup()
        await vacuum.check_missing()
        await vacuum.check_orphans()

        assert len(vacuum.orphaned) == 2
        assert len(vacuum.out_of_date) == 0
        assert len(vacuum.missing) == 24

        async def ___test():
            assert await search.get_doc_count(container) == 13
            assert (
                await search.get_doc_count(index_name=content_index_name) == 12
            )  # noqa

        await run_with_retries(___test, requester)

        await tm.abort(txn=txn)


@pytest.mark.skipif(DATABASE == "DUMMY", reason="Not for dummy db")
async def test_reindexes_moved_content(es_requester):
    async with es_requester as requester:
        resp1, _ = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Folder", "id": "foobar"}),
        )
        resp2, _ = await requester(
            "POST",
            "/db/guillotina/foobar",
            data=json.dumps({"@type": "Folder", "id": "foobar"}),
        )
        resp3, _ = await requester(
            "POST",
            "/db/guillotina/foobar/foobar",
            data=json.dumps({"@type": "Folder", "id": "foobar"}),
        )

        container, request, txn, tm = await setup_txn_on_container(requester)
        search = get_utility(ICatalogUtility)
        index_name = await search.get_container_index_name(container)

        async def _test():
            assert await search.get_doc_count(container) == 3
            result = await search.get_connection().get(
                index=index_name, doc_type="_all", id=resp3["@uid"]
            )
            assert result is not None

        await run_with_retries(_test, requester)

        # mess with index data to make it look like it was moved
        await search.get_connection().update(
            index=index_name,
            id=resp1["@uid"],
            doc_type=DOC_TYPE,
            body={
                "doc": {
                    "path": "/moved-foobar",
                    "parent_uuid": "FOOOBBAR MOVED TO NEW PARENT",
                }
            },
        )
        await search.get_connection().update(
            index=index_name,
            id=resp2["@uid"],
            doc_type=DOC_TYPE,
            body={"doc": {"path": "/moved-foobar/foobar"}},
        )
        await search.get_connection().update(
            index=index_name,
            id=resp3["@uid"],
            doc_type=DOC_TYPE,
            body={"doc": {"path": "/moved-foobar/foobar/foobar"}},
        )

        async def _test():
            result = await search.get_connection().get(
                index=index_name,
                doc_type="_all",
                id=resp3["@uid"],
                stored_fields="path",
            )
            assert result["fields"]["path"] == ["/moved-foobar/foobar/foobar"]
            result = await search.get_connection().get(
                index=index_name,
                doc_type="_all",
                id=resp1["@uid"],
                stored_fields="path,parent_uuid",
            )
            assert result["fields"]["path"] == ["/moved-foobar"]
            assert result["fields"]["parent_uuid"] == ["FOOOBBAR MOVED TO NEW PARENT"]

        await run_with_retries(_test, requester)

        await asyncio.sleep(2)

        vacuum = Vacuum(txn, tm, container)
        await vacuum.setup()
        await vacuum.check_missing()

        assert len(vacuum.orphaned) == 0
        assert len(vacuum.missing) == 1

        await asyncio.sleep(2)

        async def __test():
            result = await search.get_connection().get(
                index=index_name,
                doc_type="_all",
                id=resp3["@uid"],
                stored_fields="path,parent_uuid",
            )
            assert result["fields"]["path"] == ["/foobar/foobar/foobar"]
            result = await search.get_connection().get(
                index=index_name,
                doc_type="_all",
                id=resp1["@uid"],
                stored_fields="path,parent_uuid",
            )
            assert result["fields"]["path"] == ["/foobar"]
            assert (
                result["fields"]["parent_uuid"] != "FOOOBBAR MOVED TO NEW PARENT"
            )  # noqa

        await run_with_retries(__test, requester)

        await tm.abort(txn=txn)


@pytest.mark.skipif(DATABASE == "DUMMY", reason="Not for dummy db")
async def test_vacuum_with_multiple_containers(es_requester):
    async with es_requester as requester:

        # create another container, force to iterate differently
        _, status = await requester(
            "POST", "/db", data=json.dumps({"@type": "Container", "id": "foobar"})
        )
        assert status == 200
        await add_content(requester, num_items=100)

        search = get_utility(ICatalogUtility)
        container, request, txn, tm = await setup_txn_on_container(requester)
        task_vars.request.set(request)

        vacuum = Vacuum(txn, tm, container)
        await vacuum.setup()
        await vacuum.check_missing()
        await vacuum.check_orphans()

        async def ___test():
            assert await search.get_doc_count(container) == 1010

        await run_with_retries(___test, requester)

        await tm.abort(txn=txn)
