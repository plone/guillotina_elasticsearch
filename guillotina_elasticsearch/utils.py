from aioelasticsearch import exceptions
from guillotina.component import get_adapter
from guillotina.component import get_utilities_for
from guillotina.component import get_utility
from guillotina.content import get_all_possible_schemas_for_type
from guillotina.content import IResourceFactory
from guillotina.interfaces import ICatalogUtility
from guillotina.schema.interfaces import ICollection
from guillotina.utils.misc import get_current_container
from guillotina_elasticsearch.interfaces import IIndexActive
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.interfaces import SUB_INDEX_SEPERATOR

import asyncio
import guillotina.directives
import logging


logger = logging.getLogger("guillotina_elasticsearch")


class NoopResponse:
    def write(self, *args, **kwargs):
        pass


noop_response = NoopResponse()


async def safe_es_call(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except exceptions.ConnectionError:
        logger.warning("elasticsearch not installed", exc_info=True)
    except (exceptions.RequestError, exceptions.NotFoundError, RuntimeError):
        pass
    except exceptions.TransportError as e:
        logger.warning("Transport Error", exc_info=e)


def get_migration_lock(name):
    loop = asyncio.get_event_loop()
    key = "_es_migration_lock__" + name
    if not hasattr(loop, key):
        setattr(loop, key, asyncio.Lock())
    return getattr(loop, key)


def find_index_manager(content=None, parent=None):
    if parent is None:
        content = getattr(content, "__parent__", None)
    else:
        content = parent
    while content:
        if IIndexActive.providedBy(content):
            return get_adapter(content, IIndexManager)
        content = getattr(content, "__parent__", None)


async def get_installed_sub_indexes(container):
    search = get_utility(ICatalogUtility)
    im = get_adapter(container, IIndexManager)
    index_name = await im.get_index_name()

    results = {}
    try:
        all_aliases = await search.get_connection().indices.get_alias(
            name=index_name + "__*"
        )
    except exceptions.NotFoundError:
        return results
    for index, data in all_aliases.items():
        for name in data["aliases"].keys():
            results[name] = index

    return results


async def get_content_sub_indexes(container, path=None):
    search = get_utility(ICatalogUtility)
    im = get_adapter(container, IIndexManager)
    index_name = await im.get_index_name()
    query = {
        "size": 50,
        "query": {
            "constant_score": {
                "filter": {"bool": {"must": [{"exists": {"field": "elastic_index"}}]}}
            }
        },
    }
    if path is not None:
        query["query"]["constant_score"]["filter"]["bool"]["must"].append(
            {"term": {"path": path}}
        )
        query["query"]["constant_score"]["filter"]["bool"]["must"].append(
            {"range": {"depth": {"gte": path.count("/") + 1}}}
        )
    conn = search.get_connection()
    q_result = await conn.search(
        index=index_name,
        _source=False,
        stored_fields="elastic_index,path",
        body=query,
        scroll="1m",
    )
    indexes = [
        {
            "path": item["fields"]["path"][0],
            "oid": item["_id"],
            "index": item["fields"]["elastic_index"][0],
        }
        for item in q_result["hits"]["hits"]
    ]

    if len(q_result["hits"]["hits"]) >= 50:
        q_result = await conn.scroll(scroll_id=q_result["_scroll_id"], scroll="1m")
        [
            indexes.append(
                {
                    "path": item["fields"]["path"][0],
                    "oid": item["_id"],
                    "index": item["fields"]["elastic_index"][0],
                }
            )
            for item in q_result["hits"]["hits"]
        ]
    return indexes


async def get_all_indexes_identifier(container=None, index_manager=None):
    if index_manager is None:
        index_manager = get_adapter(container, IIndexManager)
    index_name = await index_manager.get_index_name()
    return "{},{}{}*".format(index_name, index_name, SUB_INDEX_SEPERATOR)


async def get_index_for(context, container=None):
    im = find_index_manager(parent=context)
    if im is None:
        if container is None:
            container = get_current_container()
        im = get_adapter(container, IIndexManager)
    return await im.get_index_name()


_stored_multi_valued = {}


def _is_multi_valued(check_field_name):
    if len(_stored_multi_valued) == 0:
        # load types and cache, once
        for name, _ in get_utilities_for(IResourceFactory):
            # For each type
            for schema in get_all_possible_schemas_for_type(name):
                index_fields = guillotina.directives.merged_tagged_value_dict(
                    schema, guillotina.directives.index.key
                )
                for field_name, catalog_info in index_fields.items():
                    index_name = catalog_info.get("index_name", field_name)
                    try:
                        field = schema[field_name]
                        _stored_multi_valued[index_name] = ICollection.providedBy(
                            field
                        )  # noqa
                    except KeyError:
                        _stored_multi_valued[index_name] = False

    if check_field_name in _stored_multi_valued:
        return _stored_multi_valued[check_field_name]
    return False


def format_hit(item):
    data = item.pop("_source", {})
    for key, val in item.get("fields", {}).items():
        container_data = data
        if isinstance(val, list):
            if not _is_multi_valued(key):
                if len(val) == 1:
                    val = val[0]
                elif len(val) == 0:
                    val = None
        if "." in key:
            name, key = key.split(".", 1)
            if name not in container_data:
                container_data[name] = {}
            container_data = container_data[name]
        container_data[key] = val
    return data
