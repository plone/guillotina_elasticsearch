from aioelasticsearch import exceptions
from guillotina.component import get_adapter
from guillotina.component import get_utility
from guillotina.utils import get_current_request
from guillotina.interfaces import ICatalogUtility
from guillotina_elasticsearch.interfaces import IIndexActive
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.interfaces import SUB_INDEX_SEPERATOR

import asyncio
import logging


logger = logging.getLogger('guillotina_elasticsearch')

class NoopResponse:
    def write(self, *args, **kwargs):
        pass


noop_response = NoopResponse()


async def safe_es_call(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except exceptions.ConnectionError:
        logger.warning('elasticsearch not installed', exc_info=True)
    except (exceptions.RequestError, exceptions.NotFoundError,
            RuntimeError):
        pass
    except exceptions.TransportError as e:
        logger.warning('Transport Error', exc_info=e)


def get_migration_lock(name):
    loop = asyncio.get_event_loop()
    key = '_es_migration_lock__' + name
    if not hasattr(loop, key):
        setattr(loop, key, asyncio.Lock())
    return getattr(loop, key)


def find_index_manager(content=None, parent=None):
    if parent is None:
        content = getattr(content, '__parent__', None)
    else:
        content = parent
    while content:
        if IIndexActive.providedBy(content):
            return get_adapter(content, IIndexManager)
        content = getattr(content, '__parent__', None)


async def get_installed_sub_indexes(container):
    search = get_utility(ICatalogUtility)
    im = get_adapter(container, IIndexManager)
    index_name = await im.get_index_name()

    results = {}
    try:
        all_aliases = await search.conn.indices.get_alias(
            name=index_name + '__*')
    except exceptions.NotFoundError:
        return results
    for index, data in all_aliases.items():
        for name in data['aliases'].keys():
            results[name] = index

    return results


async def get_content_sub_indexes(container, path=None):
    search = get_utility(ICatalogUtility)
    im = get_adapter(container, IIndexManager)
    index_name = await im.get_index_name()
    query = {
        "query": {
            "constant_score": {
                "filter" : {
                    "bool" : {
                        "must": [{
                            "exists": {
                                "field": "elastic_index"
                            }
                        }]
                    }
                }
            }
        }
    }
    if path is not None:
        query['query']['constant_score']['filter']['bool']['must'].append({
            "term": {
                "path": path
            }
        })
        query['query']['constant_score']['filter']['bool']['must'].append({
            "range": {
                "depth": {"gte": path.count('/') + 1}
            }
        })
    results = await search.conn.search(
        index=index_name, _source=False,
        stored_fields='elastic_index,path', body=query)
    indexes = []
    for item in results['hits']['hits']:
        indexes.append({
            'path': item['fields']['path'][0],
            'index': item['fields']['elastic_index'][0]
        })
    return indexes


async def get_all_indexes_identifier(container=None, index_manager=None):
    if index_manager is None:
        index_manager = get_adapter(container, IIndexManager)
    index_name = await index_manager.get_index_name()
    return '{},{}{}*'.format(
        index_name, index_name, SUB_INDEX_SEPERATOR)


async def get_index_for(context, container=None, request=None):
    im = find_index_manager(parent=context)
    if im is None:
        if container is None:
            if request is None:
                request = get_current_request()
            container = request.container
        im = get_adapter(container, IIndexManager)
    return await im.get_index_name()
