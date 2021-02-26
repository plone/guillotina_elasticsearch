from guillotina.directives import index_field
from guillotina.interfaces import IContainer
from guillotina.tests import utils as test_utils
from guillotina_elasticsearch.parser import Parser
from guillotina_elasticsearch.tests.utils import setup_txn_on_container

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


def test_es_field_date_parser(dummy_guillotina):
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
    assert len(qq[-1]["bool"]["should"]) == 4

    assert "range" in qq[0]
    assert "modification_date" in qq[0]["range"]
    assert "range" in qq[1]
    assert "depth" in qq[1]["range"]
    assert "lte" in qq[1]["range"]["depth"]
    assert qq[1]["range"]["depth"]["lte"] == 10


def test_parser_term_and_terms(dummy_guillotina):
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
