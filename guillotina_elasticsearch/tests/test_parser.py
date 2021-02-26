from guillotina.tests import utils as test_utils
from guillotina_elasticsearch.parser import Parser

import pytest


pytestmark = [pytest.mark.asyncio]


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
    assert len(qq[-1]["bool"]["should"]) == 4

    assert "range" in qq[0]
    assert "modification_date" in qq[0]["range"]
    assert "range" in qq[1]
    assert "depth" in qq[1]["range"]
    assert "lte" in qq[1]["range"]["depth"]
    assert qq[1]["range"]["depth"]["lte"] == 10


async def test_parser_term_and_terms(dummy_guillotina):
    content = test_utils.create_content()
    parser = Parser(None, content)
    params = {'depth__gte': '2', 'type_name': 'Item'}
    query = parser(params)
    qq = query["query"]["bool"]["must"]
    assert "_from" not in query
    assert qq[1]["term"]["type_name"] == "Item"
    params = {'depth__gte': '2', 'type_name': ['Item', 'Folder']}
    query = parser(params)
    qq = query["query"]["bool"]["must"]
    assert "Item" in qq[1]["terms"]["type_name"]
    assert "Folder" in qq[1]["terms"]["type_name"]
