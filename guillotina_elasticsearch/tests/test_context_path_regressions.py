from guillotina.content import Container
from guillotina.content import Folder
from guillotina.tests import utils as test_utils
from guillotina_elasticsearch.parser import Parser
from guillotina_elasticsearch.tests.utils import run_with_retries

import json
import pytest


pytestmark = [pytest.mark.asyncio]


@pytest.fixture
async def context_tree(es_requester):
    async with es_requester as requester:
        for path, kind in (
            ("/f1", "Folder"),
            ("/f1/a", "Example"),
            ("/f1/deep", "Folder"),
            ("/f1/deep/b", "Example"),
            ("/f10", "Folder"),
            ("/f10/other", "Example"),
        ):
            parent, _, name = path.rpartition("/")
            _, status = await requester(
                "POST",
                "/db/guillotina" + parent,
                data=json.dumps({"@type": kind, "id": name}),
                headers={"X-Wait": "10"},
            )
            assert status == 201

        async def indexed():
            response, status = await requester("GET", "/db/guillotina/@search")
            assert status == 200
            assert response["items_total"] == 6

        await run_with_retries(indexed, requester)
        yield requester


@pytest.mark.parametrize(
    "url, expected",
    [
        ("@search?path__starts=/f1", ["/f1/a", "/f1/deep", "/f1/deep/b"]),
        ("@search?path__starts=/f1/", ["/f1/a", "/f1/deep", "/f1/deep/b"]),
        ("@search?path__starts=/f1/deep", ["/f1/deep/b"]),
        ("f1/@search?path__starts=/f1/deep", ["/f1/deep/b"]),
        ("f1/@search?depth=0", []),
        ("f1/@search?path.depth=0", []),
        ("f1/@search?depth__gte=0", ["/f1/a", "/f1/deep", "/f1/deep/b"]),
        ("f1/@search?depth__lte=1", ["/f1/a", "/f1/deep"]),
        ("f1/@search?depth__gt=-1", ["/f1/a", "/f1/deep", "/f1/deep/b"]),
        ("f1/@search?depth=1", ["/f1/a", "/f1/deep"]),
        (
            "@search?path__starts=/f*",
            ["/f1/a", "/f1/deep", "/f1/deep/b", "/f10/other"],
        ),
        ("@search?path__starts=/f?", ["/f1/a", "/f1/deep", "/f1/deep/b"]),
        ("@search?path__starts=/f1//", []),
        ("f1/@search?path=/f1/deep", ["/f1/deep", "/f1/deep/b"]),
        ("f1/@search", ["/f1/a", "/f1/deep", "/f1/deep/b"]),
    ],
)
async def test_path_search_preserves_strict_descendants(context_tree, url, expected):
    response, status = await context_tree("GET", "/db/guillotina/" + url)
    assert status == 200
    assert sorted(item["path"] for item in response["items"]) == expected
    assert response["items_total"] == len(expected)


async def test_context_path_does_not_change_relevance_order(context_tree):
    # The legacy wildcard contributes the same score at every path depth.
    # With constant path scores, relevance sorting must keep the _id tie-break.
    by_id, status = await context_tree("GET", "/db/guillotina/f1/@search")
    assert status == 200
    by_score, status = await context_tree(
        "GET", "/db/guillotina/f1/@search?_sort_des=_score"
    )
    assert status == 200
    assert [item["path"] for item in by_score["items"]] == [
        item["path"] for item in by_id["items"]
    ]
    assert len({item["sort"][0] for item in by_score["items"]}) == 1


async def test_default_context_queries_keep_optimized_shape(dummy_guillotina):
    root = test_utils.create_content(factory=Container, id="root")
    folder = test_utils.create_content(factory=Folder, id="f1", parent=root)
    root_query = Parser(None, root)({})["query"]["bool"]["must"]
    assert root_query == [{"range": {"depth": {"gte": 1}}}]
    folder_query = Parser(None, folder)({})["query"]["bool"]["must"]
    assert folder_query == [
        {"range": {"depth": {"gte": 2}}},
        {"constant_score": {"filter": {"term": {"path": "/f1"}}}},
    ]
