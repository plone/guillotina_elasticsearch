# -*- coding: utf-8 -*-
from plone.server.interfaces import ICatalogDataAdapter
from plone.server.interfaces import IAbsoluteURL
from plone.server.interfaces import IResource
from plone.server.transactions import get_current_request
from plone.server.traversal import do_traverse
from plone.server.metaconfigure import rec_merge
from plone.server.events import notify
from pserver.elasticsearch.events import SearchDoneEvent
from plone.server.utils import get_content_path
from zope.security.interfaces import IInteraction
from plone.server.utils import get_content_depth
from pserver.elasticsearch.manager import ElasticSearchManager
from plone.server.interfaces import ISecurityInfo

import logging
import asyncio
import aiohttp
import json
import time


logger = logging.getLogger('pserver.elasticsearch')

MAX_RETRIES_ON_REINDEX = 5


class ElasticSearchUtility(ElasticSearchManager):

    bulk_size = 50

    async def reindex_bunk(self, site, bunk, update=False):
        if update:
            await self.update(site, bunk)
        else:
            await self.index(site, bunk)

    async def add_object(self, obj, site, loads, security=False):
        if security:
            serialization = ISecurityInfo(obj)()
        else:
            serialization = ICatalogDataAdapter(obj)()
        loads[obj.uuid] = serialization
        if len(loads) == self.bulk_size:
            await self.reindex_bunk(site, loads, update=security)
            loads.clear()

    async def reindex_recursive(self, obj, site, loads, security=False, loop=None):
        folder = obj._Folder__data
        bucket = folder._firstbucket
        if not bucket:
            return []

        tasks = []
        while bucket:
            items = await loop.run_in_executor(None, bucket.values)
            for item in items:
                await self.add_object(item, site, loads, security)
                tasks.append(self.reindex_recursive(item, site, loads, security, loop))
            bucket = bucket._next

        await asyncio.gather(*tasks)

    async def reindex_all_content(self, obj, security=False, loop=None):
        if security is False:
            await self.unindex_all_childs(obj)
        loads = {}
        if loop is None:
            loop = asyncio.get_event_loop()
        site = get_current_request().site
        await self.add_object(obj, site, loads, security)
        await self.reindex_recursive(obj, site, loads, security, loop)
        if len(loads):
            await self.reindex_bunk(site, loads)

    async def search(self, site, query):
        """
        XXX transform into el query
        """
        pass

    async def _build_security_query(
            self,
            site,
            query,
            doc_type=None,
            size=10,
            request=None):
        if query is None:
            query = {}

        q = {
            'index': self.get_index_name(site)
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
                            'minimum_number_should_match': 1
                        }
                    }
                }
            }
        }
        query = rec_merge(query, permission_query)
        # query.update(permission_query)
        q['body'] = query
        q['size'] = size
        logger.warn(q)
        return q

    async def query(
            self, site, query,
            doc_type=None, size=10, request=None):
        """
        transform into query...
        right now, it's just passing through into elasticsearch
        """
        t1 = time.time()
        if request is None:
            request = get_current_request()
        q = await self._build_security_query(
            site, query, doc_type, size, request)
        result = await self.conn.search(**q)
        items = []
        site_url = IAbsoluteURL(site, request)()
        for item in result['hits']['hits']:
            data = item['_source']
            data.update({
                '@absolute_url': site_url + data.get('path', ''),
                '@type': data.get('portal_type'),
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

    async def get_by_uuid(self, site, uuid):
        query = {
            'filter': {
                'term': {
                    'uuid': uuid
                }
            }
        }
        return await self.query(site, query, site)

    async def get_object_by_uuid(self, site, uuid):
        result = await self.get_by_uuid(site, uuid)
        if result['items_count'] == 0 or result['items_count'] > 1:
            raise AttributeError('Not found a unique object')

        path = result['members'][0]['path']
        obj = do_traverse(site, path)
        return obj

    async def get_by_type(self, site, doc_type, query={}):
        return await self.query(site, query, doc_type=doc_type)

    async def get_by_path(
            self, site, path, depth=-1, query={}, doc_type=None, size=10):
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
            query = rec_merge(query, path_query)
            # We need the local roles

        return await self.query(site, query, doc_type, size=size)

    async def unindex_all_childs(self, resource):
        if type(resource) is str:
            path = resource
            depth = path.count('/') + 1
        else:
            path = get_content_path(resource)
            depth = get_content_depth(resource)
            depth += 1
        request = get_current_request()
        index_name = self.get_index_name(request.site)
        path_query = {
            'query': {
                'bool': {
                    'must': [
                        {
                            'term':
                                {'path': path}
                        },
                        {
                            'range':
                                {'depth': {'gte': depth}}
                        }
                    ]
                }
            }
        }
        conn_es = await self.conn.transport.get_connection()
        async with conn_es._session.post(
                    conn_es._base_url + index_name + '/_delete_by_query',
                    data=json.dumps(path_query)
                ) as resp:
            result = await resp.json()
            logger.warn('Deleted %d childs' % result['deleted'])

    async def get_folder_contents(self, site, parent_uuid, doc_type=None):
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
        return await self.query(site, query, doc_type)

    async def bulk_insert(self, index_name, bulk_data, idents, count=0):
        result = {}
        try:
            print("Indexing %d" % len(idents))
            print(" Size %d" % len(json.dumps(bulk_data)))
            result = await self.conn.bulk(
                index=index_name, doc_type=None,
                body=bulk_data)
        except aiohttp.errors.ClientResponseError:
            count += 1
            if count > MAX_RETRIES_ON_REINDEX:
                logger.error('Could not index ' + ' '.join(idents))
            await asyncio.sleep(1.0)
            result = await self.bulk_insert(index_name, bulk_data, idents, count)
        return result

    async def index(self, site, datas):
        """ If there is request we get the site from there """
        if len(datas) > 0:
            bulk_data = []
            idents = []
            result = {}
            index_name = self.get_index_name(site)
            version = self.get_version(site)
            real_index_name = index_name + '_' + str(version)
            for ident, data in datas.items():
                bulk_data.extend([{
                    'index': {
                        '_index': index_name,
                        '_type': data['portal_type'],
                        '_id': ident
                    }
                }, data])
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

    async def update(self, site, datas):
        """ If there is request we get the site from there """
        if len(datas) > 0:
            bulk_data = []
            idents = []
            result = {}
            index_name = self.get_index_name(site)
            version = self.get_version(site)
            real_index_name = index_name + '_' + str(version)
            for ident, data in datas.items():
                bulk_data.extend([{
                    'update': {
                        '_index': index_name,
                        '_type': data['portal_type'],
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

    async def remove(self, site, uids):
        """List of UIDs to remove from index.

        It will remove all the childs on the index"""
        if len(uids) > 0:
            index_name = self.get_index_name(site)
            version = self.get_version(site)
            real_index_name = index_name + '_' + str(version)
            bulk_data = []
            for uid, portal_type, content_path in uids:
                bulk_data.append({
                    'delete': {
                        '_index': real_index_name,
                        '_id': uid,
                        '_type': portal_type
                    }
                })
                await self.unindex_all_childs(content_path)
            await self.conn.bulk(index=index_name, body=bulk_data)