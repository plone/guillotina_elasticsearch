# -*- coding: utf-8 -*-
from aioes import Elasticsearch
from aioes.exception import ConnectionError
from aioes.exception import RequestError
from aioes.exception import TransportError
from plone.server.catalog.catalog import DefaultSearchUtility
from plone.server.interfaces import IDataBase
from pserver.elasticsearch.schema import get_mappings
from plone.server.transactions import get_current_request
from plone.server.catalog.interfaces import ICatalogDataAdapter
from plone.server.interfaces import IResource
from plone.server.traversal import do_traverse
from plone.server.interfaces import IAbsoluteURL
from concurrent.futures import ThreadPoolExecutor
import asyncio

import logging


logger = logging.getLogger(__name__)
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
            }
        }
    }
}


class ElasticSearchUtility(DefaultSearchUtility):

    bulk_size = 50
    initialized = False
    executor = ThreadPoolExecutor(max_workers=1)

    def __init__(self, settings):
        self.settings = settings
        # self.index_index = settings['index_name']
        # self.doc_type = settings['doc_type']
        self.bulk_size = settings.get('bulk_size', 50)

    async def initialize(self, app=None):
        # No asyncio loop to run
        self.app = app
        self.conn = self.get_connection()
        # For each site create the index
        self.mappings = get_mappings()
        self.settings = DEFAULT_SETTINGS
        for db_name, db in self.app:
            if not IDataBase.providedBy(db):
                continue
            for site_name, site in db:
                try:
                    await self.conn.indices.create(site_name)
                    await self.conn.indices.close(site_name)
                    await self.conn.indices.put_settings(self.settings, site_name)
                    for key, value in self.mappings.items():
                        await self.conn.indices.put_mapping(site_name, key, value)
                    await self.conn.indices.open(site_name)
                except TransportError:
                    pass
                except ConnectionError:
                    logger.warn('elasticsearch not installed', exc_info=True)
                    return
                except RequestError:
                    return

        self.initialized = True

    def get_connection(self):
        return Elasticsearch(**self.settings['connection_settings'])

    def reindexContentAndSubcontent(self, obj, loads):
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
                yield from self.reindexContentAndSubcontent(value, loads)

    async def reindexAllContent(self, obj):
        loads = {}
        request = get_current_request()
        for bunk in self.reindexContentAndSubcontent(obj, loads):
            await self.index(bunk, request._site_id)
        await self.index(loads, request._site_id)

    async def search(self, query, site_id=None, doc_type=None):
        if query is None:
            query = {}

        request = get_current_request()
        if site_id is None:
            site_id = request._site_id

        q = {
            'index': site_id,
        }

        if doc_type is not None:
            q['doc_type'] = doc_type

        users = []
        roles = []
        for user in request.security.participations:
            users.append(user.principal.id)
            roles.extend([key for key, value in user.principal._roles.items()
                          if value])
            if (hasattr(request, '_cache_groups') and
                    hasattr(user.principal, '_groups')):
                for group in user.principal._groups:
                    users.append(group)
                    roles.extend([
                        key for key, value in
                        request._cache_groups[group]._roles.items() if value])

        # We got all users and roles
        # roles: the roles we have global (for the groups and user own)
        # users: users and groups

        permission_query = {
            'query': {
                'bool': {
                    'minimum_number_should_match': 1,
                    'should': [
                        {
                            'terms': {
                                'accessRoles': roles
                            }
                        },
                        {
                            'terms': {
                                'accessUsers': users
                            }
                        }
                    ],
                    'must_not': [
                        {
                            'terms': {
                                'denyedRoles': roles
                            }
                        },
                        {
                            'terms': {
                                'denyedUsers': users
                            }
                        }
                    ]
                }
            }
        }

        query.update(permission_query)

        q['body'] = query
        logger.warn(q)
        result = await self.conn.search(**q)
        items = []
        site_url = IAbsoluteURL(request.site, request)()
        for item in result['hits']['hits']:
            data = item['_source']
            data.update({
                '@absolute_url': site_url + data.get('path', ''),
                '@type': data.get('portal_type'),
            })
            items.append(data)
        return {
            'items_count': result['hits']['total'],
            'member': items
        }

    async def getByUUID(self, uuid, site_id):
        query = {
            'filter': {
                'term': {
                    'uuid': uuid
                }
            }
        }
        return await self.search(query, site_id)

    async def getObjectByUUID(self, uuid, site_id):
        result = await self.getByUUID(uuid, site_id)
        if result['items_count'] == 0 or result['items_count'] > 1:
            raise AttributeError('Not found a unique object')

        path = result['members'][0]['path']
        request = get_current_request()
        obj = do_traverse(request.site, path)
        return obj

    async def getByType(self, doc_type, site_id, query={}):
        return await self.search(query, site_id, doc_type=doc_type)

    async def getByPath(self, path, depth, site_id, doc_type=None):
        query = {
            'match': {
                'path': path
            }
        }
        return await self.searchByType(doc_type, site_id, query=query)

    async def getFolderContents(self, parent_uuid, site_id, doc_type=None):
        query = {
            'query': {
                'filtered': {
                    'filter': {
                        'term': {
                            'parent': parent_uuid
                        }
                    },
                    'query': {
                        'match_all': {}
                    }
                }
            }
        }
        return await self.searchByType(doc_type, site_id, query=query)

    async def index(self, datas, site_id):
        while not self.initialized:
            await asyncio.sleep(1.0)

        if len(datas) > 0:
            bulk_data = []

            for ident, data in datas.items():
                bulk_data.extend([{
                    'index': {
                        '_index': site_id,
                        '_type': data['portal_type'],
                        '_id': ident
                    }
                }, data])
                if len(bulk_data) % self.bulk_size == 0:
                    await self.conn.bulk(
                        index=site_id, doc_type=None,
                        body=bulk_data)
                    bulk_data = []

            if len(bulk_data) > 0:
                await self.conn.bulk(
                    index=site_id, doc_type=None,
                    body=bulk_data)

    async def remove(self, uids, site_id):
        """List of UIDs to remove from index."""
        if len(uids) > 0:
            bulk_data = []
            for uid, portal_type in uids:
                bulk_data.append({
                    'delete': {
                        '_index': site_id,
                        '_id': uid,
                        '_type': portal_type
                    }
                })
            await self.conn.bulk(
                index=site_id, body=bulk_data)

    async def create_index(self, site_id):
        try:
            await self.conn.indices.create(site_id)
            await self.conn.indices.close(site_id)
            await self.conn.indices.put_settings(self.settings, site_id)
            for key, value in self.mappings.items():
                await self.conn.indices.put_mapping(site_id, key, value)
            await self.conn.indices.open(site_id)
        except TransportError:
            pass
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            return
        except RequestError:
            return

    async def remove_index(self, site_id):
        try:
            await self.conn.indices.close(site_id)
            await self.conn.indices.delete(site_id)
        except TransportError:
            pass
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            return
        except RequestError:
            return
