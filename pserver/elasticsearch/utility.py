# -*- coding: utf-8 -*-
from aioes import Elasticsearch
from aioes.exception import ConnectionError
from aioes.exception import RequestError
from aioes.exception import TransportError
from plone.server import app_settings
from plone.server.catalog.catalog import DefaultSearchUtility
from plone.server.interfaces import ICatalogDataAdapter
from plone.server.interfaces import IAbsoluteURL
from plone.server.interfaces import IResource
from plone.server.transactions import get_current_request
from plone.server.traversal import do_traverse
from pserver.elasticsearch.schema import get_mappings
from plone.server.metaconfigure import rec_merge
from plone.server.events import notify
from pserver.elasticsearch.events import SearchDoneEvent

import logging
import asyncio
import aiohttp
import json


logger = logging.getLogger('pserver.elasticsearch')

MAX_RETRIES_ON_REINDEX = 5

DEFAULT_SETTINGS = {
    "analysis": {
        "analyzer": {
            "path_analyzer": {
                "tokenizer": "path_tokenizer"
            }
        },
        "tokenizer": {
            "path_tokenizer": {
                "type": "path_hierarchy",
                "delimiter": "/"
            }
        }
    }
}


class ElasticSearchUtility(DefaultSearchUtility):

    bulk_size = 50

    def __init__(self, settings):
        self._conn = None

    @property
    def bulk_size(self):
        return self.settings.get('bulk_size', 50)

    @property
    def settings(self):
        return app_settings['elasticsearch']

    @property
    def conn(self):
        if self._conn is None:
            self._conn = Elasticsearch(**self.settings['connection_settings'])
        return self._conn

    async def initialize(self, app):
        self.app = app

    def get_index_name(self, site):
        try:
            return site['_registry']['el_index_name']
        except KeyError:
            return app_settings['elasticsearch'].get('index_name_prefix', 'plone-') + site.id

    def set_index_name(self, site, name):
        site['_registry']['el_index_name'] = name

    def reindex_recursive(self, obj, loads):
        loads[obj.uuid] = ICatalogDataAdapter(obj)()
        if len(loads) == self.bulk_size:
            yield loads
            loads.clear()

        try:
            items = obj.items()
        except AttributeError:
            return
        for key, value in items:
            if IResource.providedBy(value):
                yield from self.reindex_recursive(value, loads)

    async def reindex_all_content(self, site):
        loads = {}
        for bunk in self.reindex_recursive(site, loads):
            await self.index(site, bunk)

        await self.index(site, loads)

    async def search(self, site, query):
        """
        XXX transform into el query
        """
        pass

    async def query(self, site, query, doc_type=None, size=10):
        """
        transform into query...
        right now, it's just passing through into elasticsearch
        """
        if query is None:
            query = {}

        q = {
            'index': self.get_index_name(site)
        }

        if doc_type is not None:
            q['doc_type'] = doc_type

        users = []
        roles = []
        request = get_current_request()
        for user in request.security.participations:
            users.append(user.principal.id)
            roles.extend([key for key, value in user.principal._roles.items()
                          if value])

            user_groups = getattr(user.principal, '_groups',
                                  getattr(user.principal, 'groups', [])
                                  )
            if hasattr(request, '_cache_groups'):
                for group in user_groups:
                    users.append(group)
                    roles.extend([
                        key for key, value in
                        request._cache_groups[group]._roles.items() if value])

        # We got all users and roles
        # roles: the roles we have global (for the groups and user own)
        # users: users and groups

        should_list = [{'match': {'access_roles': x}} for x in roles]
        should_list.extend([{'match': {'access_users': x}} for x in users])
        mustnot_list = [{'match': {'denyed_roles': x}} for x in roles]
        mustnot_list.extend([{'match': {'denyed_users': x}} for x in users])

        permission_query = {
            'query': {
                'bool': {
                    'minimum_number_should_match': 1,
                    'should': should_list,
                    'must_not': mustnot_list
                }
            }
        }
        query = rec_merge(query, permission_query)
        # query.update(permission_query)
        q['body'] = query
        q['size'] = size
        logger.warn(q)
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
        await notify(SearchDoneEvent(
            users, query, result['hits']['total'], request))
        return final

    async def get_by_uuid(self, site, uuid):
        query = {
            'filter': {
                'term': {
                    'uuid': uuid
                }
            }
        }
        return await self.query(site, query)

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
            query = rec_merge(query, path_query)
        return await self.query(site, query, doc_type, size=size)

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
            bulk_insert(index_name, buld_data, idents, count)

    async def index(self, site, datas):

        if len(datas) > 0:
            bulk_data = []
            idents = []
            index_name = self.get_index_name(site)
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
                    await self.bulk_insert(index_name, bulk_data, idents)
                    idents = []
                    bulk_data = []

            if len(bulk_data) > 0:
                await self.bulk_insert(index_name, bulk_data, idents)
            if 'errors' in result and result['errors']:
                logger.error(json.dumps(result['items']))
            return result

    async def remove(self, site, uids):
        """List of UIDs to remove from index."""
        if len(uids) > 0:
            index_name = self.get_index_name(site)
            bulk_data = []
            for uid, portal_type in uids:
                bulk_data.append({
                    'delete': {
                        '_index': index_name,
                        '_id': uid,
                        '_type': portal_type
                    }
                })
            await self.conn.bulk(index=index_name, body=bulk_data)

    async def initialize_catalog(self, site):
        mappings = get_mappings()
        index_name = self.get_index_name(site)
        index_settings = DEFAULT_SETTINGS.copy()
        index_settings.update(app_settings.get('index', {}))
        try:
            await self.conn.indices.create(index_name)
            await self.conn.indices.close(index_name)
            await self.conn.indices.put_settings(index_settings, index_name)
            for key, value in mappings.items():
                await self.conn.indices.put_mapping(index_name, key, value)
            await self.conn.indices.open(index_name)
            self.set_index_name(site, index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            return
        except RequestError:
            return

    async def remove_catalog(self, site):
        index_name = self.get_index_name(site)
        try:
            await self.conn.indices.close(index_name)
            await self.conn.indices.delete(index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            return
        except RequestError:
            return
