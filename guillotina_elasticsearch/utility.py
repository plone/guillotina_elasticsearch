# -*- coding: utf-8 -*-
from guillotina import configure
from guillotina.async import IAsyncUtility
from guillotina.component import getUtility
from guillotina.event import notify
from guillotina.interfaces import IAbsoluteURL
from guillotina.interfaces import IApplication
from guillotina.interfaces import ICatalogDataAdapter
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IContainer
from guillotina.interfaces import IInteraction
from guillotina.interfaces import ISecurityInfo
from guillotina.traversal import do_traverse
from guillotina.utils import get_content_depth
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina.utils import merge_dicts
from guillotina_elasticsearch.events import SearchDoneEvent
from guillotina_elasticsearch.manager import ElasticSearchManager

import aiohttp
import asyncio
import json
import logging
import time
import uuid


logger = logging.getLogger('guillotina_elasticsearch')

MAX_RETRIES_ON_REINDEX = 5
REINDEX_LOCK = False


class IElasticSearchUtility(ICatalogUtility, IAsyncUtility):
    pass


@configure.utility(provides=IElasticSearchUtility)
class ElasticSearchUtility(ElasticSearchManager):

    bulk_size = 50

    async def reindex_bunk(self, container, bunk, update=False, response=None):
        if update:
            await self.update(container, bunk)
        else:
            await self.index(container, bunk, response=response)

    async def add_object(
            self, obj, container, loads, security=False, response=None):
        if not self.enabled:
            return
        global REINDEX_LOCK
        serialization = None
        while len(loads) > 300:
            if response is not None:
                response.write(b'Buffer too big waiting\n')
            await asyncio.sleep(10)
        if response is not None and hasattr(obj, 'id'):
            response.write(
                b'Object %s Security %r Buffer %d\n' %
                (obj.id.encode('utf-8'), security, len(loads)))
        try:
            if security:
                serialization = ISecurityInfo(obj)()
            else:
                serialization = await ICatalogDataAdapter(obj)()
            loads[obj.uuid] = serialization
        except TypeError:
            pass

        if len(loads) >= self.bulk_size and REINDEX_LOCK is False:
            REINDEX_LOCK = True
            if response is not None:
                response.write(b'Going to reindex\n')
            to_index = loads.copy()
            await self.reindex_bunk(
                container, to_index, update=security, response=response)
            if response is not None:
                response.write(b'Indexed %d\n' % len(loads))
            for key in to_index.keys():
                del loads[key]
            REINDEX_LOCK = False

    async def walk_brothers(self, bucket, loop, executor):
        for item in await loop.run_in_executor(executor, bucket.values):
            yield item

    async def reindex_recursive(
            self, obj, container, loads, security=False, loop=None,
            executor=None, response=None):
        if not hasattr(obj, '_Folder__data'):
            return
        folder = obj._Folder__data
        bucket = folder._firstbucket
        if not bucket:
            return

        tasks = []
        while bucket:
            async for item in self.walk_brothers(bucket, loop, executor):
                await self.add_object(
                    obj=item,
                    container=container,
                    loads=loads,
                    security=security,
                    response=response)
                await asyncio.sleep(0)
                if IContainer.providedBy(item) and len(item):
                    tasks.append(self.reindex_recursive(
                        obj=item,
                        container=container,
                        loads=loads,
                        security=security,
                        loop=loop,
                        executor=executor,
                        response=response))
            bucket = bucket._next

        await asyncio.gather(*tasks)

    async def reindex_all_content(
            self, obj, security=False, loop=None, response=None):
        """ We can reindex content or security for an object or
        a specific query
        """
        if not self.enabled:
            return
        if security is False:
            await self.unindex_all_childs(obj, response=None, future=False)
        # count_objects = await self.count_operation(obj)
        loads = {}
        if loop is None:
            loop = asyncio.get_event_loop()
        request = get_current_request()
        executor = getUtility(IApplication, name='root').executor
        container = request.container
        await self.add_object(
            obj=obj,
            container=container,
            loads=loads,
            security=security,
            response=response)
        await self.reindex_recursive(
            obj=obj,
            container=container,
            loads=loads,
            security=security,
            loop=loop,
            executor=executor,
            response=response)
        if len(loads):
            await self.reindex_bunk(container, loads, security, response=response)

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
        tdif = t1 - time.time()
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

    async def get_by_type(self, container, doc_type, query={}):
        return await self.query(container, query, doc_type=doc_type)

    async def get_by_path(
            self, container, path, depth=-1, query={}, doc_type=None, size=10):
        if type(path) is not str:
            path = get_content_path(path)

        if path is not None and path != '/':
            path_query = {
                'query': {
                    'bool': {
                        'must': [
                            {
                                'match':
                                    {'path': path}
                            }
                        ]
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

    async def unindex_all_childs(self, resource, response=None, future=True):
        if type(resource) is str:
            path = resource
            depth = path.count('/') + 1
        else:
            path = get_content_path(resource)
            depth = get_content_depth(resource)
            depth += 1
        if response is not None:
            response.write(b'Removing all childs of %s' % path.encode('utf-8'))
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
                'match':
                    {'path': path}
            })
            path_query['query']['bool']['must'].append({
                'range':
                    {'depth': {'gte': depth}}
            })

        if future:
            _id = 'unindex_all_childs-' + uuid.uuid4().hex
            request._futures.update({_id: self.call_unindex_all_childs(index_name, path_query)})
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

    async def bulk_insert(
            self, index_name, bulk_data, idents, count=0, response=None):
        result = {}
        try:
            if response is not None:
                response.write(
                    b'Indexing %d Size %d\n' %
                    (len(idents), len(json.dumps(bulk_data)))
                )
            result = await self.conn.bulk(
                index=index_name, doc_type=None,
                body=bulk_data)
            if response is not None:
                response.write(b'Indexed')
        except aiohttp.errors.ClientResponseError as e:
            count += 1
            if count > MAX_RETRIES_ON_REINDEX:
                if response is not None:
                    response.write(
                        b'Could not index %s\n' %
                        str(e).encode('utf-8')
                    )
                logger.error('Could not index ' + ' '.join(idents) + ' ' + str(e))
            else:
                await asyncio.sleep(1.0)
                result = await self.bulk_insert(index_name, bulk_data, idents, count)
        except aiohttp.errors.ClientOSError as e:
            count += 1
            if count > MAX_RETRIES_ON_REINDEX:
                if response is not None:
                    response.write(
                        b'Could not index %s\n' %
                        str(e).encode('utf-8')
                    )
                logger.error('Could not index ' + ' '.join(idents) + ' ' + str(e))
            else:
                await asyncio.sleep(1.0)
                result = await self.bulk_insert(index_name, bulk_data, idents, count)

        return result

    async def index(self, container, datas, response=None):
        """ If there is request we get the container from there """
        if not self.enabled:
            return
        if len(datas) > 0:
            bulk_data = []
            idents = []
            result = {}
            index_name = await self.get_index_name(container)
            version = await self.get_version(container)
            real_index_name = index_name + '_' + str(version)
            for ident, data in datas.items():
                bulk_data.extend([{
                    'index': {
                        '_index': index_name,
                        '_type': data['type_name'],
                        '_id': ident
                    }
                }, data])
                idents.append(ident)
                if len(bulk_data) % (self.bulk_size * 2) == 0:
                    result = await self.bulk_insert(
                        real_index_name, bulk_data, idents, response=response)
                    idents = []
                    bulk_data = []

            if len(bulk_data) > 0:
                result = await self.bulk_insert(
                    real_index_name, bulk_data, idents, response=response)
            if 'errors' in result and result['errors']:
                logger.error(json.dumps(result['items']))
            return result

    async def update(self, container, datas):
        """ If there is request we get the container from there """
        if not self.enabled:
            return
        if len(datas) > 0:
            bulk_data = []
            idents = []
            result = {}
            index_name = await self.get_index_name(container)
            version = await self.get_version(container)
            real_index_name = index_name + '_' + str(version)
            for ident, data in datas.items():
                bulk_data.extend([{
                    'update': {
                        '_index': index_name,
                        '_type': data['type_name'],
                        '_id': ident
                    }
                }, {'doc': data}])
                idents.append(ident)
                if len(bulk_data) % (self.bulk_size * 2) == 0:
                    result = await self.bulk_insert(real_index_name, bulk_data, idents)
                    idents = []
                    bulk_data = []

            if len(bulk_data) > 0:
                result = await self.bulk_insert(real_index_name, bulk_data, idents)
            if 'errors' in result and result['errors']:
                logger.error(json.dumps(result['items']))
            return result

    async def remove(self, container, uids):
        """List of UIDs to remove from index.

        It will remove all the childs on the index"""
        if not self.enabled:
            return
        if len(uids) > 0:
            index_name = await self.get_index_name(container)
            version = await self.get_version(container)
            real_index_name = index_name + '_' + str(version)
            bulk_data = []
            for uid, type_name, content_path in uids:
                bulk_data.append({
                    'delete': {
                        '_index': real_index_name,
                        '_id': uid,
                        '_type': type_name
                    }
                })
                await self.unindex_all_childs(content_path)
            await self.conn.bulk(index=index_name, body=bulk_data)
