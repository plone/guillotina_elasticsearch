# -*- coding: utf-8 -*-
from guillotina import configure
from guillotina.async import IAsyncUtility
from guillotina.event import notify
from guillotina.interfaces import IAbsoluteURL
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IInteraction
from guillotina.traversal import do_traverse
from guillotina.utils import get_content_depth
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina.utils import merge_dicts
from guillotina_elasticsearch.events import SearchDoneEvent
from guillotina_elasticsearch.manager import ElasticSearchManager
from guillotina_elasticsearch.utils import noop_response

import aiohttp
import asyncio
import json
import logging
import time
import uuid


logger = logging.getLogger('guillotina_elasticsearch')

MAX_RETRIES_ON_REINDEX = 5
MAX_MEMORY = 0.9


class IElasticSearchUtility(ICatalogUtility, IAsyncUtility):
    pass


@configure.utility(provides=IElasticSearchUtility)
class ElasticSearchUtility(ElasticSearchManager):

    bulk_size = 75
    index_count = 0

    async def reindex_all_content(
            self, obj, security=False, response=noop_response):
        from guillotina_elasticsearch.reindex import Reindexer
        reindexer = Reindexer(self, obj, response=response,
                              reindex_security=security)
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
            request=None):
        if query is None:
            query = {}

        q = {
            'index': await self.get_index_name(container)
        }

        if doc_type is not None:
            q['doc_type'] = doc_type

        # The users who has plone.AccessContent permission by prinperm
        # The roles who has plone.AccessContent permission by roleperm
        users = []
        roles = []

        if request is None:
            request = get_current_request()
        interaction = IInteraction(request)

        for user in interaction.participations:
            users.append(user.principal.id)
            users.extend(['group:' + g for g in user.principal.groups])
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
        logger.warn(q)
        return q

    async def add_security_query(self, query, request=None):
        users = []
        roles = []
        if request is None:
            request = get_current_request()
        interaction = IInteraction(request)

        for user in interaction.participations:
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

        if 'query' not in query:
            query['query'] = {}
        if 'bool' not in query['query']:
            query['query']['bool'] = {}
        if 'filter' not in query['query']['bool']:
            query['query']['bool']['filter'] = {}

        query['query']['bool']['filter'] = {
            'bool': {
                'should': should_list,
                'minimum_should_match': 1
            }
        }

        return query

    async def query(
            self, container, query,
            doc_type=None, size=10, request=None):
        """
        transform into query...
        right now, it's just passing through into elasticsearch
        """
        t1 = time.time()
        if request is None:
            request = get_current_request()
        q = await self._build_security_query(
            container, query, doc_type, size, request)
        result = await self.conn.search(**q)
        items = []
        container_url = IAbsoluteURL(container, request)()
        for item in result['hits']['hits']:
            data = item['_source']
            data.update({
                '@absolute_url': container_url + data.get('path', ''),
                '@type': data.get('type_name'),
            })
            items.append(data)
        final = {
            'items_count': result['hits']['total'],
            'member': items
        }
        if 'aggregations' in result:
            final['aggregations'] = result['aggregations']
        if 'suggest' in result:
            final['suggest'] = result['suggest']
        tdif = time.time() - t1
        print('Time ELASTIC %f' % tdif)
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
        obj = do_traverse(container, path)
        return obj

    async def get_by_type(self, container, doc_type, query={}, size=10):
        return await self.query(container, query, doc_type=doc_type, size=size)

    async def get_by_path(
            self, container, path, depth=-1, query={}, doc_type=None, size=10):
        if type(path) is not str:
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

        return await self.query(container, query, doc_type, size=size)

    async def call_unindex_all_childs(self, index_name, path_query):
        conn_es = await self.conn.transport.get_connection()
        async with conn_es._session.post(
                    conn_es._base_url.human_repr() + index_name + '/_delete_by_query',
                    data=json.dumps(path_query)
                ) as resp:
            result = await resp.json()
            if 'deleted' in result:
                logger.warn('Deleted %d childs' % result['deleted'])
                logger.warn('Deleted %s ' % json.dumps(path_query))
            else:
                logger.warn('Wrong deletion of childs' + json.dumps(result))

    async def unindex_all_childs(self, resource, index_name=None, response=noop_response, future=True):
        if type(resource) is str:
            path = resource
            depth = path.count('/') + 1
        else:
            path = get_content_path(resource)
            depth = get_content_depth(resource)
            depth += 1
        response.write(b'Removing all children of %s' % path.encode('utf-8'))

        request = None
        if index_name is None:
            request = get_current_request()
            index_name = await self.get_index_name(request.container)

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

        if future:
            if request is None:
                request = get_current_request()
            _id = 'unindex_all_childs-' + uuid.uuid4().hex
            request._futures.update({
                _id: self.call_unindex_all_childs(index_name, path_query)})
        else:
            await self.call_unindex_all_childs(index_name, path_query)

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
                index=index_name, doc_type=None,
                body=bulk_data)
        except aiohttp.client_exceptions.ClientResponseError as e:
            count += 1
            if count > MAX_RETRIES_ON_REINDEX:
                response.write(b'Could not index %s\n' % str(e).encode('utf-8'))
                logger.error('Could not index ' + ' '.join(idents) + ' ' + str(e))
            else:
                await asyncio.sleep(1.0)
                result = await self.bulk_insert(index_name, bulk_data, idents, count)
        except aiohttp.client_exceptions.ClientOSError as e:
            count += 1
            if count > MAX_RETRIES_ON_REINDEX:
                response.write(b'Could not index %s\n' % str(e).encode('utf-8'))
                logger.error('Could not index ' + ' '.join(idents) + ' ' + str(e))
            else:
                await asyncio.sleep(1.0)
                result = await self.bulk_insert(index_name, bulk_data, idents, count)

        return result

    async def index(self, container, datas, response=noop_response, flush_all=False,
                    index_name=None, request=None):
        """ If there is request we get the container from there """
        if not self.enabled or len(datas) == 0:
            return

        check_next = False
        if index_name is None:
            check_next = True
            index_name = await self.get_index_name(container, request=request)

        bulk_data = []
        idents = []
        result = {}
        for ident, data in datas.items():
            bulk_data.extend([{
                'index': {
                    '_index': index_name,
                    '_type': data['type_name'],
                    '_id': ident
                }
            }, data])
            idents.append(ident)
            if not flush_all and len(bulk_data) % (self.bulk_size * 2) == 0:
                result = await self.bulk_insert(
                    index_name, bulk_data, idents, response=response)
                idents = []
                bulk_data = []

        if len(bulk_data) > 0:
            result = await self.bulk_insert(
                index_name, bulk_data, idents, response=response)
        if 'errors' in result and result['errors']:
            logger.error(json.dumps(result))

        if check_next:
            # also need to call on next index while it's running...
            next_index_name = await self.get_next_index_name(container, request=request)
            if next_index_name:
                async with self._migration_lock:
                    await self.index(
                        container, datas, response=response, flush_all=flush_all,
                        index_name=next_index_name, request=request)

        return result

    async def update(self, container, datas, response=noop_response, flush_all=False):
        """ If there is request we get the container from there """
        if not self.enabled:
            return
        if len(datas) > 0:
            bulk_data = []
            idents = []
            result = {}
            index_name = await self.get_index_name(container)

            for ident, data in datas.items():
                bulk_data.extend([{
                    'update': {
                        '_index': index_name,
                        '_type': data['type_name'],
                        '_id': ident
                    }
                }, {'doc': data}])
                idents.append(ident)
                if not flush_all and len(bulk_data) % (self.bulk_size * 2) == 0:
                    result = await self.bulk_insert(
                        index_name, bulk_data, idents, response=response)
                    idents = []
                    bulk_data = []

            if len(bulk_data) > 0:
                result = await self.bulk_insert(
                    index_name, bulk_data, idents, response=response)
            if 'errors' in result and result['errors']:
                logger.error(json.dumps(result))
            return result

    async def remove(self, container, uids, index_name=None, request=None, future=True):
        """List of UIDs to remove from index.

        It will remove all the childs on the index"""
        if not self.enabled:
            return

        check_next = False
        if index_name is None:
            check_next = True
            index_name = await self.get_index_name(container, request=request)

        if len(uids) > 0:
            bulk_data = []
            for uid, type_name, content_path in uids:
                bulk_data.append({
                    'delete': {
                        '_index': index_name,
                        '_id': uid,
                        '_type': type_name
                    }
                })
                await self.unindex_all_childs(content_path, index_name=index_name,
                                              future=future)
            await self.conn.bulk(index=index_name, body=bulk_data)

        if check_next:
            # also need to call on next index while it's running...
            next_index_name = await self.get_next_index_name(container,
                                                             request=request)
            if next_index_name:
                async with self._migration_lock:
                    await self.remove(container, uids, next_index_name,
                                      request=request, future=future)

    async def get_doc_count(self, container, index_name=None):
        if index_name is None:
            index_name = await self.get_index_name(container)
        result = await self.conn.count(index=index_name)
        return result['count']

    async def refresh(self, container, index_name=None):
        if index_name is None:
            index_name = await self.get_index_name(container)
        await self.conn.indices.refresh(index=index_name)
