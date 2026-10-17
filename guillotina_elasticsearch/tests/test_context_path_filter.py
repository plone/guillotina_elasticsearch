from guillotina_elasticsearch.tests.utils import run_with_retries

import asyncio
import json
import pytest


pytestmark = [pytest.mark.asyncio]


async def test_root_search_returns_all_items(es_requester):
    async with es_requester as requester:
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Folder", "title": "f1", "id": "f1"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/f1",
            data=json.dumps({"@type": "Example", "title": "a", "id": "a"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Example", "title": "b", "id": "b"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)

        async def _check():
            resp, status = await requester("GET", "/db/guillotina/@search")
            assert status == 200
            assert resp["items_total"] == 3

        await run_with_retries(_check, requester)


async def test_folder_search_only_returns_descendants(es_requester):
    async with es_requester as requester:
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Folder", "title": "f1", "id": "f1"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/f1",
            data=json.dumps({"@type": "Example", "title": "a", "id": "a"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Example", "title": "b", "id": "b"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)

        async def _check():
            resp, status = await requester("GET", "/db/guillotina/f1/@search")
            assert status == 200
            assert resp["items_total"] == 1
            assert resp["items"][0]["id"] == "a"

        await run_with_retries(_check, requester)


async def test_sibling_folders_are_separated(es_requester):
    async with es_requester as requester:
        for fid in ("f1", "f2"):
            resp, status = await requester(
                "POST",
                "/db/guillotina/",
                data=json.dumps({"@type": "Folder", "title": fid, "id": fid}),
                headers={"X-Wait": "10"},
            )
            assert status == 201
            resp, status = await requester(
                "POST",
                f"/db/guillotina/{fid}",
                data=json.dumps({"@type": "Example", "title": fid, "id": "item"}),
                headers={"X-Wait": "10"},
            )
            assert status == 201
        await asyncio.sleep(2)

        async def _check():
            for fid in ("f1", "f2"):
                resp, status = await requester("GET", f"/db/guillotina/{fid}/@search")
                assert status == 200
                assert resp["items_total"] == 1
                assert resp["items"][0]["path"] == f"/{fid}/item"

        await run_with_retries(_check, requester)
