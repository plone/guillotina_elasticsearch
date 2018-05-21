# -*- coding: utf-8 -*-
from aioelasticsearch import Elasticsearch
from guillotina import app_settings
from guillotina import configure
from guillotina.catalog.catalog import DefaultSearchUtility
from guillotina.component import get_adapter
from guillotina.event import notify
from guillotina.interfaces import IAbsoluteURL
from guillotina.interfaces import IFolder
from guillotina.interfaces import IInteraction
from guillotina.utils import get_content_depth
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina.utils import merge_dicts
from guillotina.utils import navigate_to
from guillotina_elasticsearch.events import SearchDoneEvent
from guillotina_elasticsearch.exceptions import QueryErrorException
from guillotina_elasticsearch.interfaces import DOC_TYPE
from guillotina_elasticsearch.interfaces import IElasticSearchUtility
from guillotina_elasticsearch.interfaces import IIndexActive
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.utils import find_index_manager
from guillotina_elasticsearch.utils import get_content_sub_indexes
from guillotina_elasticsearch.utils import noop_response
from guillotina_elasticsearch.utils import safe_es_call
from os.path import join

import aiohttp
import asyncio
import elasticsearch.exceptions
import json
import logging
import time


logger = logging.getLogger('guillotina_elasticsearch')

MAX_RETRIES_ON_REINDEX = 5


@configure.utility(provides=IElasticSearchUtility)
class ElasticSearchUtility(DefaultSearchUtility):

    index_count = 0

    def __init__(self, settings={}, loop=None):
        self.loop = loop
        self._conn = None

    @property
    def bulk_size(self):
        return self.settings.get('bulk_size', 50)

    @property
    def settings(self):
        return app_settings.get('elasticsearch', {})

    @property
    def conn(self):
        if self._conn is None:
            self._conn = Elasticsearch(
                loop=self.loop, **self.settings['connection_settings'])
        return self._conn

    @property
    def enabled(self):
        return len(self.settings.get('connection_settings', {}).get('hosts', [])) > 0

    async def initialize(self, app):
        self.app = app

    async def finalize(self, app):
        if self._conn is not None:
            await self._conn.close()

    async def initialize_catalog(self, container):
        if not self.enabled:
            return
        await self.remove_catalog(container)
        index_manager = get_adapter(container, IIndexManager)

        index_name = await index_manager.get_index_name()
        real_index_name = await index_manager.get_real_index_name()

        await self.create_index(real_index_name, index_manager)
        await self.conn.indices.put_alias(
            name=index_name, index=real_index_name)
        await self.conn.indices.close(real_index_name)

        await self.conn.indices.open(real_index_name)
        await self.conn.cluster.health(wait_for_status='yellow')  # pylint: disable=E1123

    async def create_index(self, real_index_name, index_manager, settings=None, mappings=None):
        if settings is None:
            settings = await index_manager.get_index_settings()
        if mappings is None:
            mappings = await index_manager.get_mappings()
        settings = {
            'settings': settings,
            'mappings': {
                DOC_TYPE: mappings
            }
        }
        await self.conn.indices.create(real_index_name, settings)

    async def _delete_index(self, im):
        index_name = await im.get_index_name()
        real_index_name = await im.get_real_index_name()
        await safe_es_call(self.conn.indices.close, real_index_name)
        await safe_es_call(self.conn.indices.delete_alias, real_index_name, index_name)
        await safe_es_call(self.conn.indices.delete, real_index_name)
        await safe_es_call(self.conn.indices.delete, index_name)
        migration_index = await im.get_migration_index_name()
        if migration_index:
            await safe_es_call(self.conn.indices.close, migration_index)
            await safe_es_call(self.conn.indices.delete, migration_index)

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
        return await self.conn.indices.stats(
            await self.get_container_index_name(container))

    async def reindex_all_content(
            self, obj, security=False, response=noop_response, request=None):
        from guillotina_elasticsearch.reindex import Reindexer
        reindexer = Reindexer(self, obj, response=response,
                              reindex_security=security, request=request)
        await reindexer.reindex(obj)

    async def search(self, container, query):
        """
        XXX transform into el query
        """
        pass

    async def _build_security_query(
            self,
            container,
            query,
            doc_type=None,
            size=10,
            request=None,
            scroll=None):
        if query is None:
            query = {}

        q = {}

        # The users who has plone.AccessContent permission by prinperm
        # The roles who has plone.AccessContent permission by roleperm
        users = []
        roles = []

        if request is None:
            request = get_current_request()
        interaction = IInteraction(request)

        for user in interaction.participations:  # pylint: disable=E1133
            users.append(user.principal.id)
            users.extend(user.principal.groups)
            roles_dict = interaction.global_principal_roles(
                user.principal.id,
                user.principal.groups)
            roles.extend([key for key, value in roles_dict.items()
                          if value])
        # We got all users and roles
        # users: users and groups

        should_list = [{'match': {'access_roles': x}} for x in roles]
        should_list.extend([{'match': {'access_users': x}} for x in users])

        permission_query = {
            'query': {
                'bool': {
                    'filter': {
                        'bool': {
                            'should': should_list,
                            'minimum_should_match': 1
                        }
                    }
                }
            }
        }
        query = merge_dicts(query, permission_query)
        # query.update(permission_query)
        q['body'] = query
        q['size'] = size

        if scroll:
            q['scroll'] = scroll

        logger.debug(q)
        return q

    def _get_items_from_result(self, container, request, result):
        items = []
        container_url = IAbsoluteURL(container, request)()
        for item in result['hits']['hits']:
            data = item.pop('_source', {})
            for key, val in item.get('fields', {}).items():
                container_data = data
                if isinstance(val, list):
                    if len(val) == 1:
                        val = val[0]
                    else:
                        val = None
                if '.' in key:
                    name, key = key.split('.', 1)
                    if name not in container_data:
                        container_data[name] = {}
                    container_data = container_data[name]
                container_data[key] = val
            data.update({
                '@absolute_url': container_url + data.get('path', ''),
                '@type': data.get('type_name'),
                '@uid': item['_id'],
                '@name': data.get('id', data.get('path', '').split('/')[-1])
            })
            sort_value = item.get('sort')
            if sort_value:
                data.update({'sort': sort_value})
            items.append(data)
        return items

    async def query(
            self, container, query,
            doc_type=None, size=10, request=None, scroll=None, index=None):
        """
        transform into query...
        right now, it's just passing through into elasticsearch
        """
        if index is None:
            index = await self.get_container_index_name(container)
        t1 = time.time()
        if request is None:
            request = get_current_request()
        q = await self._build_security_query(
            container, query, doc_type, size, request, scroll)
        result = await self.conn.search(index=index, **q)
        if result.get('_shards', {}).get('failed', 0) > 0:
            logger.warning(f'Error running query: {result["_shards"]}')
            error_message = 'Unknown'
            for failure in result["_shards"].get('failures') or []:
                error_message = failure['reason']
            return QueryErrorException(reason=error_message)
        items = self._get_items_from_result(container, request, result)
        final = {
            'items_count': result['hits']['total'],
            'member': items
        }
        if 'aggregations' in result:
            final['aggregations'] = result['aggregations']
        if 'suggest' in result:
            final['suggest'] = result['suggest']
        if 'profile' in result:
            final['profile'] = result['profile']
        if '_scroll_id' in result:
            final['_scroll_id'] = result['_scroll_id']

        tdif = time.time() - t1
        logger.debug(f'Time ELASTIC {tdif}')
        await notify(SearchDoneEvent(
            query, result['hits']['total'], request, tdif))
        return final

    async def get_by_uuid(self, container, uuid):
        query = {
            'filter': {
                'term': {
                    'uuid': uuid
                }
            }
        }
        return await self.query(container, query, container)

    async def get_by_uuids(self, container, uuids, doc_type=None):
        query = {
            "query": {
                "bool": {
                    "must": [{
                        "terms":
                            {"uuid": uuids}
                    }]
                }
            }
        }
        return await self.query(container, query, doc_type)

    async def get_object_by_uuid(self, container, uuid):
        result = await self.get_by_uuid(container, uuid)
        if result['items_count'] == 0 or result['items_count'] > 1:
            raise AttributeError('Not found a unique object')

        path = result['members'][0]['path']
        obj = await navigate_to(container, path)
        return obj

    async def get_by_type(self, container, doc_type, query=None, size=10):
        if query is None:
            query = {}
        return await self.query(container, query, doc_type=doc_type, size=size)

    async def get_by_path(
            self, container, path, depth=-1, query=None,
            doc_type=None, size=10, scroll=None):
        if query is None:
            query = {}
        if not isinstance(path, str):
            path = get_content_path(path)

        if path is not None and path != '/':
            path_query = {
                'query': {
                    'bool': {
                        'must': [{
                            'match': {'path': path}
                        }]
                    }
                }
            }
            if depth > -1:
                query['query']['bool']['must'].append({
                    'range':
                        {'depth': {'gte': depth}}
                })
            query = merge_dicts(query, path_query)
            # We need the local roles

        return await self.query(container, query, doc_type,
                                size=size, scroll=scroll)

    async def get_path_query(self, resource, response=noop_response):
        if isinstance(resource, str):
            path = resource
            depth = path.count('/') + 1
        else:
            path = get_content_path(resource)
            depth = get_content_depth(resource)
            depth += 1

        path_query = {
            'query': {
                'bool': {
                    'must': [
                    ]
                }
            }
        }
        if path != '/':
            path_query['query']['bool']['must'].append({
                'term':
                    {'path': path}
            })
            path_query['query']['bool']['must'].append({
                'range':
                    {'depth': {'gte': depth}}
            })
        return path_query

    async def unindex_all_children(self, container, resource,
                                   index_name=None, response=noop_response):
        content_path = get_content_path(resource)
        response.write(b'Removing all children of %s' % content_path.encode('utf-8'))
        # use future here because this can potentially take some
        # time to clean up indexes, etc
        asyncio.ensure_future(
            self.call_unindex_all_children(container, index_name, content_path))

    async def call_unindex_all_children(self, container, index_name, content_path):
        # first, find any indexes connected with this path so we can delete them.
        for index_data in await get_content_sub_indexes(container, content_path):
            try:
                all_aliases = await self.conn.indices.get_alias(
                    name=index_data['index'])
            except elasticsearch.exceptions.NotFoundError:
                continue
            for index, data in all_aliases.items():
                for name in data['aliases'].keys():
                    # delete alias
                    try:
                        await self.conn.indices.close(index)
                        await self.conn.indices.delete_alias(index, name)
                        await self.conn.indices.delete(index)
                    except elasticsearch.exceptions.NotFoundError:
                        pass

        path_query = await self.get_path_query(content_path)
        conn_es = await self.conn.transport.get_connection()
        async with conn_es.session.post(
                join(conn_es.base_url.human_repr(),
                     index_name, '_delete_by_query'),
                data=json.dumps(path_query),
                headers={
                    'Content-Type': 'application/json'
                }) as resp:
            result = await resp.json()
            if 'deleted' in result:
                logger.debug(f'Deleted {result["deleted"]} children')
                logger.debug(f'Deleted {json.dumps(path_query)}')
            else:
                self.log_result(result, 'Deletion of children')

    async def update_by_query(self, query):
        request = get_current_request()
        indexes = await self.get_current_indexes(request.container)
        return await self._update_by_query(query, ','.join(indexes))

    async def _update_by_query(self, query, index_name):
        conn_es = await self.conn.transport.get_connection()
        url = join(conn_es.base_url.human_repr(), index_name,
                   '_update_by_query?conflicts=proceed')
        async with conn_es.session.post(
                url, data=json.dumps(query),
                headers={
                    'Content-Type': 'application/json'
                }) as resp:
            result = await resp.json()
            if 'updated' in result:
                logger.debug(f'Updated {result["updated"]} children')
                logger.debug(f'Updated {json.dumps(query)} ')
            else:
                self.log_result(result, 'Updating children')
            import pdb; pdb.set_trace()
            return result

    async def get_folder_contents(self, container, parent_uuid, doc_type=None):
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'term': {
                            'parent_uuid': parent_uuid
                        }
                    },
                    'query': {
                        'match_all': {}
                    }
                }
            }
        }
        return await self.query(container, query, doc_type)

    async def bulk_insert(self, index_name, bulk_data, idents, count=0,
                          response=noop_response):
        result = {}
        try:
            response.write(b'Indexing %d' % (len(idents),))
            result = await self.conn.bulk(
                index=index_name, doc_type=DOC_TYPE,
                body=bulk_data)
        except aiohttp.client_exceptions.ClientResponseError as e:
            count += 1
            if count > MAX_RETRIES_ON_REINDEX:
                response.write(b'Could not index %s\n' % str(e).encode('utf-8'))
                logger.error('Could not index ' + ' '.join(idents) + ' ' + str(e))
            else:
                await asyncio.sleep(0.5)
                result = await self.bulk_insert(index_name, bulk_data, idents, count)
        except aiohttp.client_exceptions.ClientOSError as e:
            count += 1
            if count > MAX_RETRIES_ON_REINDEX:
                response.write(b'Could not index %s\n' % str(e).encode('utf-8'))
                logger.error('Could not index ' + ' '.join(idents) + ' ' + str(e))
            else:
                await asyncio.sleep(0.5)
                result = await self.bulk_insert(index_name, bulk_data, idents, count)

        return result

    async def get_current_indexes(self, container):
        index_manager = get_adapter(container, IIndexManager)
        return await index_manager.get_indexes()

    async def index(self, container, datas, response=noop_response, flush_all=False,
                    index_name=None, request=None):
        """ If there is request we get the container from there """
        if not self.enabled or len(datas) == 0:
            return

        if index_name is None:
            indexes = await self.get_current_indexes(container)
        else:
            indexes = [index_name]

        bulk_data = []
        idents = []
        result = {}
        for ident, data in datas.items():
            item_indexes = data.pop('__indexes__', indexes)
            for index in item_indexes:
                bulk_data.extend([{
                    'index': {
                        '_index': index,
                        '_id': ident
                    }
                }, data])
            idents.append(ident)
            if not flush_all and len(bulk_data) % (self.bulk_size * 2) == 0:
                result = await self.bulk_insert(
                    indexes[0], bulk_data, idents, response=response)
                idents = []
                bulk_data = []

        if len(bulk_data) > 0:
            result = await self.bulk_insert(
                indexes[0], bulk_data, idents, response=response)

        self.log_result(result)

        return result

    async def update(self, container, datas, response=noop_response, flush_all=False):
        """ If there is request we get the container from there """
        if not self.enabled:
            return
        if len(datas) > 0:
            bulk_data = []
            idents = []
            result = {}
            indexes = await self.get_current_indexes(container)

            for ident, data in datas.items():
                item_indexes = data.pop('__indexes__', indexes)
                for index in item_indexes:
                    bulk_data.extend([{
                        'update': {
                            '_index': index,
                            '_id': ident,
                            '_retry_on_conflict': 3
                        }
                    }, {'doc': data}])
                idents.append(ident)
                if not flush_all and len(bulk_data) % (self.bulk_size * 2) == 0:
                    result = await self.bulk_insert(
                        indexes[0], bulk_data, idents, response=response)
                    idents = []
                    bulk_data = []

            if len(bulk_data) > 0:
                result = await self.bulk_insert(
                    indexes[0], bulk_data, idents, response=response)
            self.log_result(result)
            return result

    def log_result(self, result, label='ES Query'):
        if 'errors' in result and result['errors']:
            try:
                if result['error']['caused_by']['type'] in ('index_not_found_exception',
                                                            'cluster_block_exception'):
                    return  # ignore these...
            except KeyError:
                return
            logger.error(label + ': ' + json.dumps(result))
        else:
            logger.debug(label + ': ' + json.dumps(result))

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
                    bulk_data.append({
                        'delete': {
                            '_index': index,
                            '_id': obj.uuid
                        }
                    })
                if IFolder.providedBy(obj):
                    # only folders need to have children cleaned
                    if IIndexActive.providedBy(obj):
                        # delete this index...
                        im = get_adapter(obj, IIndexManager)
                        await self._delete_index(im)
                    else:
                        await self.unindex_all_children(
                            container, obj, index_name=','.join(item_indexes))
            await self.conn.bulk(
                index=indexes[0], body=bulk_data, doc_type=DOC_TYPE)

    async def get_doc_count(self, container, index_name=None):
        if index_name is None:
            index_manager = get_adapter(container, IIndexManager)
            index_name = await index_manager.get_real_index_name()
        result = await self.conn.count(index=index_name)
        return result['count']

    async def refresh(self, container, index_name=None):
        if index_name is None:
            index_manager = get_adapter(container, IIndexManager)
            index_name = await index_manager.get_real_index_name()
        await self.conn.indices.refresh(index=index_name)

    async def get_data(self, content, indexes=None):
        im = find_index_manager(content)
        # attempt to find index manager on parent of object we're
        # indexing and mark the object with the indexes we want
        # to store it in
        if im is not None:
            data = await super().get_data(content, indexes, im.get_schemas())
            data['__indexes__'] = await im.get_indexes()
        else:
            data = await super().get_data(content, indexes)
        return data
