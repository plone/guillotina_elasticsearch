from guillotina import configure
from guillotina.content import Folder
from guillotina.interfaces import IResource
from guillotina_elasticsearch.directives import index
from guillotina_elasticsearch.interfaces import IContentIndex


class IUniqueIndexContent(IResource, IContentIndex):
    pass


class IIndexItemContent(IResource):
    pass


@configure.contenttype(
    type_name="UniqueIndexContent",
    schema=IUniqueIndexContent)
class UniqueIndexContent(Folder):
    index(
        schemas=[IResource],
        settings={

        }
    )


@configure.contenttype(
    type_name="IndexItemContent",
    schema=IIndexItemContent)
class IndexItemContent(Folder):
    pass
