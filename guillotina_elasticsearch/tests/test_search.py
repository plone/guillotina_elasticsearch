from datetime import datetime
from datetime import timedelta
from datetime import timezone
from guillotina.auth import authenticate_user
from guillotina.auth import set_authenticated_user
from guillotina.auth.utils import find_user
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.utils import get_authenticated_user
from guillotina_elasticsearch.parser import Parser
from guillotina_elasticsearch.tests.utils import run_with_retries
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import asyncio
import json
import pytest


pytestmark = [pytest.mark.asyncio]


async def test_indexing_and_search(es_requester):
    async with es_requester as requester:
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "Example",
                    "title": "Item1",
                    "id": "item1",
                    "categories": [
                        {"label": "term1", "number": 1.0},
                        {"label": "term2", "number": 2.0},
                    ],
                }
            ),
        )
        assert status == 201

        async def _test():
            resp, status = await requester(
                "POST", "/db/guillotina/@search", data=json.dumps({})
            )
            assert status == 200
            assert resp["items_total"] == 1
            assert resp["items"][0]["path"] == "/item1"

        await run_with_retries(_test, requester)

        # try removing now...
        await requester("DELETE", "/db/guillotina/item1")

        async def _test():
            resp, status = await requester(
                "POST", "/db/guillotina/@search", data=json.dumps({})
            )
            assert resp["items_total"] == 0

        await run_with_retries(_test, requester)


async def test_removes_all_children(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa

        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Folder", "title": "Folder1", "id": "folder1"}),
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/folder1",
            data=json.dumps({"@type": "Folder", "title": "Folder2", "id": "folder2"}),
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/folder1/folder2",
            data=json.dumps({"@type": "Folder", "title": "Folder3", "id": "folder3"}),
        )
        assert status == 201

        async def _test():
            resp, status = await requester(
                "POST", "/db/guillotina/@search", data=json.dumps({})
            )
            assert resp["items_total"] == 3
            assert resp["items"][0]["@name"]

        await run_with_retries(_test, requester)

        # try removing now...
        await requester("DELETE", "/db/guillotina/folder1")

        async def _test():
            resp, status = await requester(
                "POST", "/db/guillotina/@search", data=json.dumps({})
            )
            assert resp["items_total"] == 0

        await run_with_retries(_test, requester)


async def test_search_unrestricted(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        resp, status = await requester(
            "POST",
            "/db/guillotina/@addons",
            data=json.dumps(
                {
                    "id": "dbusers",
                }
            ),
        )
        assert status == 200

        resp, status = await requester(
            "POST",
            "/db/guillotina/users",
            headers={"X-Wait": "10"},
            data=json.dumps(
                {
                    "@type": "User",
                    "id": "foo_user",
                    "username": "foo_user",
                    "name": "Foo User",
                    "email": "foo_user@guillotina.cat",
                    "password": "foo_user1234",
                    "user_roles": ["guillotina.Member"],
                }
            ),
        )
        assert status == 201

        resp, status = await requester(
            "POST",
            "/db/guillotina/@login",
            data=json.dumps({"username": "foo_user", "password": "foo_user1234"}),
        )
        assert status == 200
        token = resp["token"]
        _, data = authenticate_user("foo_user")
        data["token"] = token
        user = await find_user(data)
        set_authenticated_user(user)
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {"@type": "Folder", "title": "Foo Folder", "id": "foo_folder"}
            ),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Item", "title": "Foo Item", "id": "foo_item"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)
        utility = get_utility(ICatalogUtility)
        parser = Parser(None, container)
        query = parser({"type_name": "Item"})
        user_auth = get_authenticated_user()
        assert user_auth.username == "foo_user"
        results = await utility.search_raw(container, query)
        # No results cause foo_user is not the owner of the item, but root
        assert results["items_total"] == 0
        # When unrestricted, event if the user foo_user is authenticated
        # Search raw get results
        results = await utility.search_raw(container, query, unrestricted=True)
        assert results["items_total"] == 1
        assert results["items"][0]["@name"] == "foo_item"


async def test_search_date(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        utility = get_utility(ICatalogUtility)
        parser = Parser(None, container)
        now = datetime.now(timezone.utc)
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Example", "title": "Item1", "id": "item1"}),
            headers={"X-Wait": "10"},
        )
        await asyncio.sleep(2)
        # Test that we can filter by date, with granularity of seconds
        assert status == 201
        query = {
            "type_name": "Example",
            "modification_date__gte": (now + timedelta(seconds=-2)).isoformat(),
        }
        query = parser(query)
        results = await utility.search_raw(container, query)
        assert results["items_total"] == 1

        query = {
            "type_name": "Example",
            "modification_date__gte": (now + timedelta(seconds=2)).isoformat(),
        }
        results = await utility.search_raw(container, parser(query))
        assert results["items_total"] == 0

        # Test with days

        assert status == 201
        query = {
            "type_name": "Example",
            "modification_date__gte": (now + timedelta(days=-1)).isoformat(),
        }
        results = await utility.search_raw(container, parser(query))
        assert results["items_total"] == 1

        query = {
            "type_name": "Example",
            "modification_date__gte": (now + timedelta(days=1)).isoformat(),
        }
        results = await utility.search_raw(container, parser(query))
        assert results["items_total"] == 0


async def test_context_search_and_count(es_requester):
    # https://github.com/plone/guillotina_elasticsearch/issues/93
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Folder", "title": "Folder1", "id": "folder"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        parent_uuid = resp["@uid"]
        await asyncio.sleep(1)
        search = get_utility(ICatalogUtility)
        assert await search.get_doc_count(container, query={"type_name": "Folder"}) == 1
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Folder", "title": "Folder1", "id": "folder2"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201

        resp, status = await requester(
            "POST",
            "/db/guillotina/folder",
            data=json.dumps({"@type": "Item", "title": "Item1", "id": "foo_item"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(1)
        assert await search.get_doc_count(container, query={"type_name": "Item"}) == 1
        resp, status = await requester(
            "POST",
            "/db/guillotina/folder2",
            data=json.dumps({"@type": "Item", "title": "Item1", "id": "foo_item"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)

        # Should only have found one result
        resp, status = await requester("GET", "/db/guillotina/folder/@search")
        assert resp["items_total"] == 1
        assert resp["items"][0]["parent_uuid"] == parent_uuid
        assert resp["items"][0]["id"] == "foo_item"

        resp, status = await requester("GET", "/db/guillotina/@search?type_name=Item")
        assert resp["items_total"] == 2
        resp, status = await requester("GET", "/db/guillotina/@count?type_name=Item")
        assert resp == 2
        resp, status = await requester("GET", "/db/guillotina/@count?type_name=Folder")
        assert resp == 2
        resp, status = await requester("GET", "/db/guillotina/@count")
        assert resp == 4
        resp, status = await requester(
            "GET", "/db/guillotina/folder/@count?type_name=Item"
        )
        assert resp == 1
        resp, status = await requester(
            "GET", "/db/guillotina/folder/@count?type_name=Folder"
        )
        assert resp == 0


async def test_or_search(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            headers={"X-Wait": "10"},
            data=json.dumps({"@type": "Example", "title": "example", "id": "item1"}),
        )
        assert status == 201

        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Folder", "title": "Folder1", "id": "folder"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201

        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Item", "title": "Item", "id": "item"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        utility = get_utility(ICatalogUtility)
        parser = Parser(None, container)
        query = parser({"type_name__or": ["Item", "Folder"]})
        await asyncio.sleep(3)
        results = await utility.search_raw(container, query)
        assert results["items_total"] == 2
        for item in results["items"]:
            assert item["@type"] in ["Item", "Folder"]

        query = parser({"type_name__or": ["Item", "Example"]})
        results = await utility.search_raw(container, query)
        assert results["items_total"] == 2
        for item in results["items"]:
            assert item["@type"] in ["Item", "Example"]


@pytest.mark.app_settings(
    {
        "applications": [
            "guillotina",
            "guillotina_elasticsearch",
            "guillotina_elasticsearch.tests.test_package",
        ]
    }
)
async def test_normalizer_analyzers_search(es_requester):
    async with es_requester as requester:
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "FooContent",
                    "title": "Item",
                    "id": "item",
                    "item_keyword": "foo_kéyword",
                    "item_text": "foo_item",
                }
            ),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)
        # Testing normalizer is working: We can search foo_keyword without the accent
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?item_keyword=foo_keyword&_metadata=item_keyword",
            headers={"X-Wait": "10"},
        )
        assert status == 200
        assert resp["items_total"] == 1
        assert resp["items"][0]["item_keyword"] == "foo_keyword"

        # and with the accent aswell
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?item_keyword=foo_kéyword&_metadata=item_keyword",
            headers={"X-Wait": "10"},
        )
        assert status == 200
        assert resp["items_total"] == 1
        assert resp["items"][0]["item_keyword"] == "foo_keyword"

        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?item_keyword__in=foo_&_metadata=item_text",
            headers={"X-Wait": "10"},
        )
        assert status == 200
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "FooContent",
                    "title": "Item",
                    "id": "item2",
                    "item_keyword": "foo_kéyword",
                    "item_text": "another_text",
                }
            ),
            headers={"X-Wait": "10"},
        )
        await asyncio.sleep(2)
        # We can sort by the new multi field raw of item_text which is a keyword
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_sort_asc=item_text.raw&_metadata=item_text",
            headers={"X-Wait": "10"},
        )
        assert resp["items_total"] == 2
        assert resp["items"][0]["item_text"] == "another_text"

        # We can search querying the new multi field raw of item_text which is a keyword
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&item_text.raw=another_text&_metadata=*",
            headers={"X-Wait": "10"},
        )
        assert resp["items_total"] == 1
        assert resp["items"][0]["item_text"] == "another_text"


@pytest.mark.app_settings(
    {
        "applications": [
            "guillotina",
            "guillotina_elasticsearch",
            "guillotina_elasticsearch.tests.test_package",
        ]
    }
)
async def test_search_fields_not_exists(es_requester):
    async with es_requester as requester:
        # Let's index a document without item_keyword
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "FooContent",
                    "title": "Item",
                    "id": "item",
                    "item_text": "foo_item",
                }
            ),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)
        # Results without the field
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_metadata=*&item_keyword=null",
        )
        assert status == 200
        assert resp["items_total"] == 1
        assert resp["items"][0]["id"] == "item"
        # Let's create a document with item_keyword
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "FooContent",
                    "title": "Item",
                    "id": "item2",
                    "item_keyword": "foo_keyword",
                    "item_text": "foo_item",
                }
            ),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)
        # Only results without item_keyword should be returned
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_metadata=*&item_keyword=null",
        )
        assert status == 200
        assert resp["items_total"] == 1
        assert resp["items"][0]["id"] == "item"
        # Let's create a document without the field item_text
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "FooContent",
                    "title": "Item",
                    "id": "item3",
                    "item_keyword": "foo_keyword",
                }
            ),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(3)
        # Only documents without item_text should be returned
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_metadata=*&item_text=null",
        )
        assert status == 200
        assert resp["items_total"] == 1
        assert resp["items"][0]["id"] == "item3"
        # Let's make sure the query is not broken when searching after the query param of =null
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_metadata=*&item_text=null&item_keyword=foo_keyword",
        )
        assert status == 200
        assert resp["items_total"] == 1
        assert resp["items"][0]["id"] == "item3"
        # Let's create another docuemnt without item_text
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "FooContent",
                    "title": "Item",
                    "id": "item4",
                    "item_keyword": "foo_keyword",
                }
            ),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(3)
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_metadata=*&item_text=null&item_keyword=foo_keyword",
        )
        assert status == 200
        assert resp["items_total"] == 2

        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_metadata=*&item_text=null&item_keyword=foo_keyword",
        )
        assert status == 200
        assert resp["items_total"] == 2

        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_metadata=*&item_keyword__not=null",
        )
        assert status == 200
        assert resp["items_total"] == 3

        # Let's create another document with item_text
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps(
                {
                    "@type": "FooContent",
                    "title": "Item",
                    "id": "item5",
                    "item_text": "foo_text",
                }
            ),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(3)

        # Let's user __or to test that null work as expected together with other values
        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=FooContent&_metadata=*&__or=item_text=null%26item_text=foo_text",
        )
        assert status == 200
        assert resp["items_total"] == 3
        expected_results_id = ["item3", "item4", "item5"]
        for item in resp["items"]:
            assert item["id"] in expected_results_id
