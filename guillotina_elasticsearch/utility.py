# -*- coding: utf-8 -*-
from elasticsearch import AsyncElasticsearch
from guillotina import app_settings
from guillotina import configure
from guillotina.catalog.catalog import DefaultSearchUtility
from guillotina.component import get_adapter
from guillotina.component import get_utility
from guillotina.event import notify
from guillotina.exceptions import ContainerNotFound
from guillotina.exceptions import RequestNotFound
from guillotina.interfaces import IFolder
from guillotina.transactions import get_transaction
from guillotina.utils import find_container
from guillotina.utils import get_content_depth
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina.utils import get_object_url
from guillotina.utils import merge_dicts
from guillotina.utils import navigate_to
from guillotina.utils import resolve_dotted_name
from guillotina.utils.misc import get_current_container
from guillotina_elasticsearch.events import SearchDoneEvent
from guillotina_elasticsearch.exceptions import ElasticsearchConflictException
from guillotina_elasticsearch.exceptions import QueryErrorException
from guillotina_elasticsearch.interfaces import DOC_TYPE
from guillotina_elasticsearch.interfaces import IConnectionFactoryUtility
from guillotina_elasticsearch.interfaces import IElasticSearchUtility  # noqa b/w compat
from guillotina_elasticsearch.interfaces import IIndexActive
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.utils import find_index_manager
from guillotina_elasticsearch.utils import format_hit
from guillotina_elasticsearch.utils import get_content_sub_indexes
from guillotina_elasticsearch.utils import noop_response
from guillotina_elasticsearch.utils import safe_es_call

import asyncio
import backoff
import elasticsearch.exceptions
import json
import logging
import time


logger = logging.getLogger("guillotina_elasticsearch")

MAX_RETRIES_ON_REINDEX = 5


@configure.utility(provides=IConnectionFactoryUtility)
class DefaultConnnectionFactoryUtility:
    """
    Default uses single connection for entire application
    """

    def __init__(self):
        self._conn = None

    def get(self, loop=None):
        if self._conn is None:
            self._conn = AsyncElasticsearch(
                loop=loop,
                **app_settings.get("elasticsearch", {}).get("connection_settings"),
            )
        return self._conn

    async def close(self, loop=None):
        if self._conn is not None:
            if loop is not None:
                asyncio.run_coroutine_threadsafe(self._conn.close(), loop)
            else:
                await self._conn.close()
            self._conn = None


class ElasticSearchUtility(DefaultSearchUtility):

    index_count = 0

    def __init__(self, settings={}, loop=None):
        self.loop = loop
        self._conn_util = None

    @property
    def bulk_size(self):
        return self.settings.get("bulk_size", 50)

    def _refresh(self):
        if not hasattr(self, "__refresh"):
            val = self.settings.get("refresh")
            if val:
                val = resolve_dotted_name(val)
            self.__refresh = val

        if self.__refresh:
            return self.__refresh()
        return False

    @property
    def settings(self):
        return app_settings.get("elasticsearch", {})

    @property
    def conn(self):
        # b/w compat
        return self.get_connection()

    def get_connection(self) -> AsyncElasticsearch:
        if self._conn_util is None:
            self._conn_util = get_utility(IConnectionFactoryUtility)
        return self._conn_util.get(loop=self.loop)

    @property
    def enabled(self):
        return len(self.settings.get("connection_settings", {}).get("hosts", [])) > 0

    async def initialize(self, app):
        self.app = app
        await self.check_supported_version()

    async def finalize(self, app):
        if self._conn_util is not None:
            await self._conn_util.close()

    async def check_supported_version(self):
        try:
            connection = self.get_connection()
            info = await connection.info()
        except Exception:
            logger.warning(
                "Could not check current es version. " "Only 6.x and 7.x are supported"
            )
            return

        es_version = info["version"]["number"]

        if not es_version.startswith("7"):
            raise Exception(f"ES cluster version not supported: {es_version}")

    async def initialize_catalog(self, container):
        if not self.enabled:
            return
        await self.remove_catalog(container)
        index_manager = get_adapter(container, IIndexManager)

        index_name = await index_manager.get_index_name()
        real_index_name = await index_manager.get_real_index_name()
        await self.create_index(real_index_name, index_manager)
        conn = self.get_connection()
        await conn.indices.put_alias(name=index_name, index=real_index_name)
        await conn.cluster.health(wait_for_status="yellow")  # pylint: disable=E1123

    async def create_index(
        self, real_index_name, index_manager, settings=None, mappings=None
    ):
        if ":" in real_index_name:
            raise Exception(f"Ivalid character ':' in index name: {real_index_name}")

        if settings is None:
            settings = await index_manager.get_index_settings()
        if mappings is None:
            mappings = await index_manager.get_mappings()

        settings = {"settings": settings, "mappings": mappings}

        conn = self.get_connection()
        await conn.indices.create(real_index_name, settings)

    async def _delete_index(self, im):
        index_name = await im.get_index_name()
        real_index_name = await im.get_real_index_name()
        conn = self.get_connection()
        await safe_es_call(conn.indices.delete_alias, real_index_name, index_name)
        await safe_es_call(conn.indices.delete, real_index_name)
        await safe_es_call(conn.indices.delete, index_name)
        migration_index = await im.get_migration_index_name()
        if migration_index:
            await safe_es_call(conn.indices.delete, migration_index)

    async def remove_catalog(self, container):
        if not self.enabled:
            return
        index_manager = get_adapter(container, IIndexManager)
        await self._delete_index(index_manager)

    async def get_container_index_name(self, container):
        index_manager = get_adapter(container, IIndexManager)
        return await index_manager.get_index_name()

    get_index_name = get_container_index_name  # b/w

    async def stats(self, container):
        conn = self.get_connection()
        return await conn.indices.stats(await self.get_container_index_name(container))

    async def reindex_all_content(
        self, obj, security=False, response=noop_response, request=None
    ):
        from guillotina_elasticsearch.reindex import Reindexer

        reindexer = Reindexer(self, obj, response=response, reindex_security=security)
        await reindexer.reindex(obj)

    async def _build_security_query(
        self, context, query, size=10, scroll=None, unrestricted=False
    ):
        if query is None:
            query = {}
        build_security_query = resolve_dotted_name(
            app_settings["elasticsearch"]["security_query_builder"]
        )
        if unrestricted:
            permission_query = {}
        else:
            permission_query = await build_security_query(context)
        result = {
            "body": merge_dicts(query, permission_query),
            "size": query.get("size", size),
        }

        if scroll:
            result["scroll"] = scroll

        logger.debug(result)
        return result

    def _get_items_from_result(self, container, request, result):
        items = []
        container_url = get_object_url(container, request)
        for item in result["hits"]["hits"]:
            data = format_hit(item)
            data.update(
                {
                    "@id": container_url + data.get("path", ""),
                    "@type": data.get("type_name"),
                    "@uid": item["_id"],
                    "@name": data.get("id", data.get("path", "").split("/")[-1]),
                }
            )
            sort_value = item.get("sort")
            if sort_value:
                data.update({"sort": sort_value})
            if "highlight" in item:
                data["@highlight"] = item["highlight"]
            items.append(data)
        return items

    async def search_raw(
        self,
        context,
        query,
        doc_type=None,
        size=10,
        request=None,
        scroll=None,
        index=None,
        unrestricted=False,
    ):
        """
        Search raw query
        """
        container = find_container(context)
        if container is None:
            raise ContainerNotFound()
        if index is None:
            index = await self.get_container_index_name(container)
        t1 = time.time()
        if request is None:
            try:
                request = get_current_request()
            except RequestNotFound:
                pass

        q = await self._build_security_query(context, query, size, scroll, unrestricted)
        q["ignore_unavailable"] = True

        logger.debug("Generated query %s", json.dumps(query))
        conn = self.get_connection()

        result = await conn.search(index=index, **q)
        if result.get("_shards", {}).get("failed", 0) > 0:
            logger.warning(f'Error running query: {result["_shards"]}')
            error_message = "Unknown"
            for failure in result["_shards"].get("failures") or []:
                error_message = failure["reason"]
            raise QueryErrorException(reason=error_message)
        items = self._get_items_from_result(container, request, result)
        items_total = result["hits"]["total"]["value"]
        final = {"items_total": items_total, "items": items}

        if "aggregations" in result:
            final["aggregations"] = result["aggregations"]
        if "suggest" in result:
            final["suggest"] = result["suggest"]
        if "profile" in result:
            final["profile"] = result["profile"]
        if "_scroll_id" in result:
            final["_scroll_id"] = result["_scroll_id"]

        tdif = time.time() - t1
        logger.debug(f"Time ELASTIC {tdif}")
        await notify(SearchDoneEvent(query, items_total, request, tdif))
        return final

    async def get_by_uuid(self, container, uuid):
        query = {"filter": {"term": {"uuid": uuid}}}
        return await self.search_raw(container, query, container)

    async def get_by_uuids(self, container, uuids, doc_type=None):
        uuid_query = self._get_type_query(doc_type)
        if uuids is not None:
            uuid_query["query"]["bool"]["must"].append({"terms": {"uuid": uuids}})
        return await self.search_raw(container, uuid_query)

    async def get_object_by_uuid(self, container, uuid):
        result = await self.get_by_uuid(container, uuid)
        if result["items_total"] == 0 or result["items_total"] > 1:
            raise AttributeError("Not found a unique object")

        path = result["items"][0]["path"]
        obj = await navigate_to(container, path)
        return obj

    def _get_type_query(self, doc_type):
        query = {"query": {"bool": {"must": []}}}

        if doc_type is not None:
            query["query"]["bool"]["must"].append({"term": {"type_name": doc_type}})
        return query

    async def get_by_type(self, container, doc_type, query=None, size=10):
        type_query = self._get_type_query(doc_type)
        if query is not None:
            type_query = merge_dicts(query, type_query)
        return await self.query(container, type_query, size=size)

    async def get_by_path(
        self,
        container,
        path,
        depth=-1,
        query=None,
        doc_type=None,
        size=10,
        scroll=None,
        index=None,
    ):
        if query is None:
            query = {}
        if not isinstance(path, str):
            path = get_content_path(path)

        path_query = self._get_type_query(doc_type)

        if path is not None and path != "/":
            path_query["query"]["bool"]["must"].append({"match": {"path": path}})

            if depth > -1:
                query["query"]["bool"]["must"].append(
                    {"range": {"depth": {"gte": depth}}}
                )

        query = merge_dicts(query, path_query)

        return await self.query(container, query, size=size, scroll=scroll, index=index)

    async def get_path_query(self, resource, response=noop_response):
        if isinstance(resource, str):
            path = resource
            depth = path.count("/") + 1
        else:
            path = get_content_path(resource)
            depth = get_content_depth(resource)
            depth += 1

        path_query = {"query": {"bool": {"must": []}}}
        if path != "/":
            path_query["query"]["bool"]["must"].append({"term": {"path": path}})
            path_query["query"]["bool"]["must"].append(
                {"range": {"depth": {"gte": depth}}}
            )
        return path_query

    async def unindex_all_children(
        self, container, resource, index_name=None, response=noop_response
    ):
        content_path = get_content_path(resource)
        response.write(b"Removing all children of %s" % content_path.encode("utf-8"))
        # use future here because this can potentially take some
        # time to clean up indexes, etc
        await self.call_unindex_all_children(container, index_name, content_path)

    @backoff.on_exception(
        backoff.constant,
        (asyncio.TimeoutError, elasticsearch.exceptions.ConnectionTimeout),
        interval=1,
        max_tries=5,
    )
    async def call_unindex_all_children(self, container, index_name, content_path):
        # first, find any indexes connected with this path so we can
        # delete them.
        conn = self.get_connection()
        sub_indexes = await get_content_sub_indexes(container, content_path)
        for index_data in sub_indexes:
            try:
                all_aliases = await conn.indices.get_alias(name=index_data["index"])
            except elasticsearch.exceptions.NotFoundError:
                continue
            for index, data in all_aliases.items():
                for name in data["aliases"].keys():
                    # delete alias
                    try:
                        await conn.indices.close(index)
                        await conn.indices.delete_alias(index, name)
                        await conn.indices.delete(index)
                    except elasticsearch.exceptions.NotFoundError:
                        pass

        path_query = await self.get_path_query(content_path)
        await self._delete_by_query(path_query, index_name)

    @backoff.on_exception(
        backoff.constant, (ElasticsearchConflictException,), interval=0.5, max_tries=5
    )
    async def _delete_by_query(self, path_query, index_name):
        conn = self.get_connection()
        result = await conn.delete_by_query(
            index_name,
            body=path_query,
            ignore_unavailable="true",
            conflicts="proceed",
        )
        if result["version_conflicts"] > 0:
            raise ElasticsearchConflictException(result["version_conflicts"], result)
        if "deleted" in result:
            logger.debug(f'Deleted {result["deleted"]} children')
            logger.debug(f"Deleted {json.dumps(path_query)}")
        else:
            self.log_result(result, "Deletion of children")

    async def update_by_query(self, query, context=None, indexes=None):
        if indexes is None:
            container = get_current_container()
            indexes = await self.get_current_indexes(container)
            if context is not None:
                for index in await get_content_sub_indexes(
                    container, get_content_path(context)
                ):
                    indexes.append(index["index"])
        return await self._update_by_query(query, ",".join(indexes))

    @backoff.on_exception(
        backoff.constant,
        (asyncio.TimeoutError, elasticsearch.exceptions.ConnectionTimeout),
        interval=1,
        max_tries=5,
    )
    async def _update_by_query(self, query, index_name):
        conn = self.get_connection()
        result = await conn.update_by_query(
            index_name,
            body=query,
            ignore_unavailable="true",
            conflicts="proceed",
        )
        if "updated" in result:
            logger.debug(f'Updated {result["updated"]} children')
            logger.debug(f"Updated {json.dumps(query)} ")
        else:
            self.log_result(result, "Updating children")
        return result

    @backoff.on_exception(
        backoff.constant,
        (asyncio.TimeoutError, elasticsearch.exceptions.ConnectionTimeout),
        interval=1,
        max_tries=5,
    )
    async def bulk_insert(
        self, index_name, bulk_data, idents, count=0, response=noop_response
    ):
        conn = self.get_connection()
        result = {}
        try:
            response.write(b"Indexing %d" % (len(idents),))
            result = await conn.bulk(
                index=index_name,
                doc_type=DOC_TYPE,
                body=bulk_data,
                refresh=self._refresh(),
            )
        except elasticsearch.exceptions.TransportError as e:
            count += 1
            if count > MAX_RETRIES_ON_REINDEX:
                response.write(b"Could not index %s\n" % str(e).encode("utf-8"))
                logger.error("Could not index " + " ".join(idents) + " " + str(e))
            else:
                await asyncio.sleep(0.5)
                result = await self.bulk_insert(index_name, bulk_data, idents, count)

        if isinstance(result, dict) and result.get("errors"):
            logger.error("Error indexing: {}".format(result))

        return result

    async def get_current_indexes(self, container):
        index_manager = get_adapter(container, IIndexManager)
        return await index_manager.get_indexes()

    async def index(
        self,
        container,
        datas,
        response=noop_response,
        flush_all=False,
        index_name=None,
        request=None,
    ):
        """If there is request we get the container from there"""
        if not self.enabled or len(datas) == 0:
            return

        if index_name is None:
            indexes = await self.get_current_indexes(container)
        else:
            indexes = [index_name]

        tid = self._get_current_tid()

        bulk_data = []
        idents = []
        result = {}
        for ident, data in datas.items():
            item_indexes = data.pop("__indexes__", indexes)
            if tid and tid > (data.get("tid") or 0):
                data["tid"] = tid
            for index in item_indexes:
                bulk_data.extend([{"index": {"_index": index, "_id": ident}}, data])
            idents.append(ident)
            if not flush_all and len(bulk_data) % (self.bulk_size * 2) == 0:
                result = await self.bulk_insert(
                    indexes[0], bulk_data, idents, response=response
                )
                idents = []
                bulk_data = []

        if len(bulk_data) > 0:
            result = await self.bulk_insert(
                indexes[0], bulk_data, idents, response=response
            )

        self.log_result(result)

        return result

    def _get_current_tid(self):
        # make sure to get current committed tid or we may be one-behind
        # for what was actually used to commit to db
        tid = None
        try:
            txn = get_transaction()
            if txn:
                tid = txn._tid
        except RequestNotFound:
            pass
        return tid

    async def update(self, container, datas, response=noop_response, flush_all=False):
        """If there is request we get the container from there"""
        if not self.enabled:
            return
        tid = self._get_current_tid()

        if len(datas) > 0:
            bulk_data = []
            idents = []
            result = {}
            indexes = await self.get_current_indexes(container)

            for ident, data in datas.items():
                item_indexes = data.pop("__indexes__", indexes)
                if tid and tid > (data.get("tid") or 0):
                    data["tid"] = tid
                for index in item_indexes:
                    bulk_data.extend(
                        [
                            {
                                "update": {
                                    "_index": index,
                                    "_id": ident,
                                    "retry_on_conflict": 3,
                                }
                            },
                            {"doc": data},
                        ]
                    )
                idents.append(ident)
                if not flush_all and len(bulk_data) % (self.bulk_size * 2) == 0:
                    result = await self.bulk_insert(
                        indexes[0], bulk_data, idents, response=response
                    )
                    idents = []
                    bulk_data = []

            if len(bulk_data) > 0:
                result = await self.bulk_insert(
                    indexes[0], bulk_data, idents, response=response
                )
            self.log_result(result)
            return result

    def log_result(self, result, label="ES Query"):
        if "errors" in result and result["errors"]:
            try:
                if result["error"]["caused_by"]["type"] in (
                    "index_not_found_exception",
                    "cluster_block_exception",
                ):
                    return  # ignore these...
            except KeyError:
                return
            logger.error(label + ": " + json.dumps(result))
        else:
            logger.debug(label + ": " + json.dumps(result))

    async def remove(self, container, objects, index_name=None, request=None):
        """List of UIDs to remove from index.

        It will remove all the children on the index"""
        if not self.enabled:
            return

        if len(objects) > 0:
            if index_name is None:
                indexes = await self.get_current_indexes(container)
            else:
                indexes = [index_name]
            bulk_data = []
            for obj in objects:
                item_indexes = indexes
                im = find_index_manager(obj)
                if im:
                    item_indexes = await im.get_indexes()
                for index in item_indexes:
                    bulk_data.append({"delete": {"_index": index, "_id": obj.__uuid__}})
                if IFolder.providedBy(obj):
                    # only folders need to have children cleaned
                    if IIndexActive.providedBy(obj):
                        # delete this index...
                        im = get_adapter(obj, IIndexManager)
                        await self._delete_index(im)
                    else:
                        await self.unindex_all_children(
                            container, obj, index_name=",".join(item_indexes)
                        )
            conn = self.get_connection()
            await conn.bulk(
                index=indexes[0],
                body=bulk_data,
                doc_type=DOC_TYPE,
                refresh=self._refresh(),
            )

    async def get_doc_count(self, container=None, index_name=None):
        if index_name is None:
            index_manager = get_adapter(container, IIndexManager)
            index_name = await index_manager.get_real_index_name()
        conn = self.get_connection()
        result = await conn.count(index=index_name)
        return result["count"]

    async def refresh(self, container=None, index_name=None):
        conn = self.get_connection()
        if index_name is None:
            index_manager = get_adapter(container, IIndexManager)
            index_name = await index_manager.get_real_index_name()
        await conn.indices.refresh(index=index_name)

    async def get_data(self, content, indexes=None):
        im = find_index_manager(content)
        # attempt to find index manager on parent of object we're
        # indexing and mark the object with the indexes we want
        # to store it in
        if im is not None:
            data = await super().get_data(content, indexes, await im.get_schemas())
            data["__indexes__"] = await im.get_indexes()
        else:
            data = await super().get_data(content, indexes)
        return data
