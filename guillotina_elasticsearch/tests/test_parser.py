from guillotina.tests import utils as test_utils
from guillotina_elasticsearch.parser import Parser

import pytest


pytestmark = [pytest.mark.asyncio]


async def _test_es_field_parser(dummy_guillotina):
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

    qq = parsed["query"]["query"]["bool"]["must"]

    assert len(qq[-1]["bool"]["should"]) == 2

    assert "range" in qq[0]
    assert "modification_date" in qq[0]["range"]
    assert "range" in qq[1]
    assert "depth" in qq[1]["range"]
    assert "lte" in qq[1]["range"]["depth"]
    assert qq[1]["range"]["depth"]["lte"] == 10
