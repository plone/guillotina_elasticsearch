from aioelasticsearch import Elasticsearch
from guillotina import app_settings
from guillotina import configure
from guillotina.content import Folder
from guillotina.exceptions import RequestNotFound
from guillotina.interfaces import IResource
from guillotina.utils import get_current_request
from guillotina_elasticsearch.directives import index
from guillotina_elasticsearch.interfaces import IConnectionFactoryUtility
from guillotina_elasticsearch.interfaces import IContentIndex
from guillotina_elasticsearch.utility import DefaultConnnectionFactoryUtility


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


@configure.utility(provides=IConnectionFactoryUtility)
class CustomConnSettingsUtility(DefaultConnnectionFactoryUtility):
    '''
    test to demonstrate using different settings from configuration
    '''

    def __init__(self):
        super().__init__()
        self._special_conn = None

    def get(self, request=None, container=None, loop=None):
        if container is None:
            try:
                request = get_current_request()
                container = getattr(request, 'container', None)
            except RequestNotFound:
                pass

        settings = app_settings.get('elasticsearch', {}).get(
            'connection_settings'
        )
        if (container is None or container.id != 'new_container' or
                'new_container_settings' not in app_settings['elasticsearch']):
            return super().get(request, container, loop)
        else:
            if self._special_conn is None:
                settings = settings.copy()
                settings.update(
                    app_settings['elasticsearch']['new_container_settings'])
                self._special_conn = Elasticsearch(loop=loop, **settings)
            return self._special_conn

    def close(self):
        super().close()
        if self._special_conn is not None:
            self._special_conn.close()
