from elasticsearch import exceptions
from guillotina.component import get_adapter
from guillotina.component import get_utilities_for
from guillotina.content import get_all_possible_schemas_for_type
from guillotina.content import IResourceFactory
from guillotina.schema.interfaces import ICollection
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


async def get_all_indexes_identifier(container=None, index_manager=None):
    if index_manager is None:
        index_manager = get_adapter(container, IIndexManager)
    index_name = await index_manager.get_index_name()
    return "{},{}{}*".format(index_name, index_name, SUB_INDEX_SEPERATOR)


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


def get_parent_by_interface(content, interface):
    """
    Return the direct parent
    """

    parts = []
    parent = getattr(content, "__parent__", None)
    while (
        content is not None
        and content.__name__ is not None
        and parent is not None
        and not interface.providedBy(content)
    ):
        parts.append(content.__name__)
        content = parent
        parent = getattr(content, "__parent__", None)
    if interface.providedBy(content):
        return content
