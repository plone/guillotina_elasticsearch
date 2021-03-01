from guillotina.component import get_utility
from guillotina.db.uid import get_short_uid
from guillotina.interfaces import ICatalogUtility
from guillotina_elasticsearch.tests.utils import run_with_retries
from guillotina_elasticsearch.tests.utils import setup_txn_on_container
from guillotina_elasticsearch.utils import get_content_sub_indexes
from guillotina_elasticsearch.utils import get_installed_sub_indexes

import aioelasticsearch
import json
import pytest


pytestmark = [pytest.mark.asyncio]


async def test_create_index(es_requester):
    async with es_requester as requester:
        resp, status = await requester(
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
        catalog = get_utility(ICatalogUtility)
        assert status == 201
        # assert indexes were created
        assert await catalog.get_connection().indices.exists_alias(
            "guillotina-db-guillotina__uniqueindexcontent-{}".format(
                get_short_uid(resp["@uid"])
            )
        )
        assert await catalog.get_connection().indices.exists(
            "1_guillotina-db-guillotina__uniqueindexcontent-{}".format(
                get_short_uid(resp["@uid"])
            )
        )


async def test_indexes_data_in_correct_indexes(es_requester):
    async with es_requester as requester:
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
        resp, status = await requester(
            "POST",
            "/db/guillotina/foobar",
            data=json.dumps(
                {
                    "@type": "IndexItemContent",
                    "title": "IndexItemContent",
                    "id": "foobar",
                }
            ),
        )
        assert status == 201
        content_index_name = (
            "guillotina-db-guillotina__uniqueindexcontent-{}".format(  # noqa
                get_short_uid(cresp["@uid"])
            )
        )
        search = get_utility(ICatalogUtility)

        async def _test():
            # should find in content index but not main index
            result = await search.get_connection().get(
                index=content_index_name, doc_type="_all", id=resp["@uid"]
            )
            assert result is not None
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await search.get_connection().get(
                    index="guillotina-guillotina", doc_type="_all", id=resp["@uid"]
                )

        await run_with_retries(_test, requester)


async def test_elastic_index_field(es_requester):
    async with es_requester as requester:
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
        resp, _ = await requester(
            "POST",
            "/db/guillotina/foobar",
            data=json.dumps(
                {
                    "@type": "IndexItemContent",
                    "title": "IndexItemContent",
                    "id": "foobar",
                }
            ),
        )
        content_index_name = (
            "guillotina-db-guillotina__uniqueindexcontent-{}".format(  # noqa
                get_short_uid(cresp["@uid"])
            )
        )
        search = get_utility(ICatalogUtility)

        async def _test():
            result = await search.get_connection().get(
                index="guillotina-db-guillotina", doc_type="_all", id=cresp["@uid"]
            )
            assert result["_source"]["elastic_index"] == content_index_name
            result = await search.get_connection().get(
                index=content_index_name, doc_type="_all", id=resp["@uid"]
            )
            assert result["_source"]["title"] == "IndexItemContent"

        await run_with_retries(_test, requester)


async def test_delete_resource(es_requester):
    async with es_requester as requester:
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

        resp, status = await requester(
            "POST",
            "/db/guillotina/foobar",
            data=json.dumps(
                {
                    "@type": "IndexItemContent",
                    "title": "IndexItemContent",
                    "id": "foobar",
                }
            ),
        )
        assert status == 201
        content_index_name = (
            "guillotina-db-guillotina__uniqueindexcontent-{}".format(  # noqa
                get_short_uid(cresp["@uid"])
            )
        )
        search = get_utility(ICatalogUtility)

        async def _test():
            # should find in content index but not main index
            result = await search.get_connection().get(
                index=content_index_name, doc_type="_all", id=resp["@uid"]
            )
            assert result is not None
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await search.get_connection().get(
                    index="guillotina-guillotina", doc_type="_all", id=resp["@uid"]
                )

        await run_with_retries(_test, requester)

        # now, delete it
        await requester("DELETE", "/db/guillotina/foobar/foobar")

        async def _test():
            # should find in content index but not main index
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await search.get_connection().get(
                    index=content_index_name, doc_type="_all", id=resp["@uid"]
                )

        await run_with_retries(_test, requester)


async def test_delete_base_removes_index_from_elastic(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)
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
        resp, _ = await requester(
            "POST",
            "/db/guillotina/foobar",
            data=json.dumps(
                {
                    "@type": "IndexItemContent",
                    "title": "IndexItemContent",
                    "id": "foobar",
                }
            ),
        )
        catalog = get_utility(ICatalogUtility)
        await requester("DELETE", "/db/guillotina/foobar")
        content_index_name = (
            "guillotina-db-guillotina__uniqueindexcontent-{}".format(  # noqa
                get_short_uid(cresp["@uid"])
            )
        )

        async def _test():
            # should find in content index but not main index
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await catalog.get_connection().get(
                    index=content_index_name, doc_type="_all", id=resp["@uid"]
                )
            with pytest.raises(aioelasticsearch.exceptions.NotFoundError):
                await catalog.get_connection().get(
                    index="guillotina-guillotina", doc_type="_all", id=cresp["@uid"]
                )

        await run_with_retries(_test, requester)

        assert not await catalog.get_connection().indices.exists_alias(
            "guillotina-db-guillotina__uniqueindexcontent-{}".format(
                get_short_uid(resp["@uid"])
            )
        )
        assert not await catalog.get_connection().indices.exists(
            "1_guillotina-db-guillotina__uniqueindexcontent-{}".format(
                get_short_uid(resp["@uid"])
            )
        )


async def test_delete_parent_gets_cleaned_up(es_requester):
    async with es_requester as requester:
        await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Folder", "id": "foobar"}),
        )
        await requester(
            "POST",
            "/db/guillotina/foobar",
            data=json.dumps(
                {
                    "@type": "UniqueIndexContent",
                    "title": "UniqueIndexContent",
                    "id": "foobar",
                }
            ),
        )
        await requester(
            "POST",
            "/db/guillotina/foobar/foobar",
            data=json.dumps(
                {
                    "@type": "IndexItemContent",
                    "title": "IndexItemContent",
                    "id": "foobar",
                }
            ),
        )
        container, request, txn, tm = await setup_txn_on_container(requester)

        async def _test():
            installed_sub_indexes = await get_installed_sub_indexes(container)
            assert len(installed_sub_indexes) == 1
            sub_indexes = await get_content_sub_indexes(container)
            assert len(sub_indexes) == 1

        await run_with_retries(_test, requester)

        await requester("DELETE", "/db/guillotina/foobar")

        async def _test():
            assert len(await get_content_sub_indexes(container)) == 0
            assert len(await get_installed_sub_indexes(container)) == 0

        await run_with_retries(_test, requester)
