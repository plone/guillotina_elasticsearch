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


async def test_context_search(es_requester):
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
