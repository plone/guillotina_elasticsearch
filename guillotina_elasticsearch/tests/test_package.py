from guillotina import configure
from guillotina.content import Resource
from guillotina.directives import index_field
from guillotina.interfaces import IContainer
from guillotina.interfaces import IResource
from guillotina.schema import TextLine
from zope.interface import implementer


class IFooContent(IResource):
    index_field(
        "item_keyword",
        type="keyword",
        normalizer="common_normalizer",
        field="item_keyword",
        store=True,
    )
    item_keyword = TextLine()

    index_field(
        "item_text",
        type="text",
        analyzer="common_analyzer",
        field="item_text",
        store=True,
        multifields={"raw": {"type": "keyword"}},
        search_analyzer="standard",
    )
    item_text = TextLine()


@implementer(IFooContent)
class FooContent(Resource):
    pass


configure.register_configuration(
    FooContent,
    dict(
        context=IContainer,
        schema=IFooContent,
        type_name="FooContent",
        behaviors=["guillotina.behaviors.dublincore.IDublinCore"],
    ),
    "contenttype",
)
