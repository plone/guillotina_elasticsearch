from guillotina.directives import index_field
from guillotina.schema import Float
from guillotina.schema import Int
from guillotina_elasticsearch.schema import get_mappings
from guillotina_elasticsearch.tests.test_package import IFooContent
from zope.interface import Interface

import pytest


pytestmark = [pytest.mark.asyncio]


class IA(Interface):
    index_field("item", field_mapping={"type": "integer"})
    item = Int()


class IB(Interface):
    index_field("item", field_mapping={"type": "float"})
    item = Float()


class IC(Interface):
    index_field("item", field_mapping={"type": "float"})
    item = Float()


async def test_get_mappings_fails_on_conflict(es_requester):
    with pytest.raises(Exception):
        # Two content types define DIFFERENT mapping for same field ->
        #   conflict!
        get_mappings(schemas=[IA, IB])


async def test_get_mappings_nofails(es_requester):
    try:
        # Two content types define SAME mapping for same field ->
        #   everything is ok
        get_mappings(schemas=[IB, IC])
    except Exception:
        pytest.fail("get_mappings() shouldn't fail " "if 'field_mapping' are the same")


async def test_get_mappings_analyzers_normalizers(es_requester):
    mappings = get_mappings(schemas=[IFooContent])
    assert mappings["properties"]["item_keyword"] == {
        "index": True,
        "normalizer": "common_normalizer",
        "type": "keyword",
        "store": True,
    }
    assert mappings["properties"]["item_text"] == {
        "index": True,
        "analyzer": "common_analyzer",
        "type": "text",
        "store": True,
        "fields": {"raw": {"type": "keyword"}},
        "search_analyzer": "standard",
    }
