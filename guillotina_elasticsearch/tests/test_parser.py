from guillotina.directives import index_field
from guillotina.interfaces import IContainer
from guillotina.tests import utils as test_utils
from guillotina_elasticsearch.parser import Parser
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

import asyncio
import json
import pytest


pytestmark = [pytest.mark.asyncio]


async def test_boolean_field(es_requester):
    async with es_requester as requester:

        @index_field.with_accessor(IContainer, "foo_bool", type="boolean")
        def index_bool(obj):
            return True

        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        params = {"depth__gte": "1", "type_name": "IItem", "foo_bool": True}
        parser = Parser(None, container)
        query = parser(params)
        qq = query["query"]["bool"]["must"]
        assert "_from" not in query
        assert qq[1]["term"]["type_name"] == "IItem"
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/boolean.html
        assert qq[2]["term"]["foo_bool"] == "true"


@pytest.mark.app_settings(
    {
        "applications": [
            "guillotina",
            "guillotina_elasticsearch",
            "guillotina_elasticsearch.tests.test_package",
        ]
    }
)
async def test_es_field_date_parser(dummy_guillotina):
    content = test_utils.create_content()
    parser = Parser(None, content)

    parsed = parser(
        {
            "modification_date__gte": "2019/10/10",
            "depth__lte": 10,
            "SearchableText": "foobar",
            "_sort_asc": "modification_date",
        }
    )
    qq = parsed["query"]["bool"]["must"]
    assert len(qq[-2]["bool"]["should"]) == 5

    assert "range" in qq[0]
    assert "modification_date" in qq[0]["range"]
    assert "range" in qq[1]
    assert "depth" in qq[1]["range"]
    assert "lte" in qq[1]["range"]["depth"]
    assert qq[1]["range"]["depth"]["lte"] == 10


async def test_parser_term_and_terms(dummy_guillotina):
    content = test_utils.create_content()
    parser = Parser(None, content)
    params = {"depth__gte": "2", "type_name": "Item"}
    query = parser(params)
    qq = query["query"]["bool"]["must"]
    assert "_from" not in query
    assert qq[1]["term"]["type_name"] == "Item"
    params = {"depth__gte": "2", "type_name": ["Item", "Folder"]}
    query = parser(params)
    qq = query["query"]["bool"]["must"]
    assert "Item" in qq[1]["terms"]["type_name"]
    assert "Folder" in qq[1]["terms"]["type_name"]
    # Check that b_start and b_size work as expected
    # https://github.com/plone/guillotina_elasticsearch/issues/93
    params = {"b_start": 5, "b_size": 10}
    query = parser(params)
    assert query["from"] == 5
    assert query["size"] == 10


async def test_parser_or_operator(es_requester):
    async with es_requester as requester:
        container, request, txn, tm = await setup_txn_on_container(requester)  # noqa
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Item", "id": "foo_item"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Item", "id": "foo_item2"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        resp, status = await requester(
            "POST",
            "/db/guillotina/",
            data=json.dumps({"@type": "Item", "id": "foo_item3"}),
            headers={"X-Wait": "10"},
        )
        assert status == 201
        await asyncio.sleep(2)
        resp, status = await requester(
            "GET", "/db/guillotina/@search?type_name=Item", headers={"X-Wait": "10"}
        )
        assert resp["items_total"] == 3

        resp, status = await requester(
            "GET",
            "/db/guillotina/@search?type_name=Item&__or=id=foo_item%26id=foo_item2",
            headers={"X-Wait": "10"},
        )
        assert resp["items_total"] == 2
