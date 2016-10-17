# -*- coding: utf-8 -*-
from aioes import Elasticsearch
from aioes.exception import ConnectionError
from aioes.exception import RequestError
from aioes.exception import TransportError
from plone.server.catalog.catalog import DefaultSearchUtility
from plone.dexterity.utils import iterSchemata
from plone.supermodel.interfaces import FIELDSETS_KEY
from plone.supermodel.interfaces import CATALOG_KEY
from plone.supermodel.interfaces import INDEX_KEY
from plone.supermodel.utils import mergedTaggedValueDict
from zope.component import getUtilitiesFor
from plone.dexterity.fti import IDexterityFTI
from plone.server.interfaces import IDataBase
from plone.dexterity.utils import iterSchemataForType
from zope.schema import getFields
from plone.server.transactions import get_current_request
from plone.server.catalog.interfaces import ICatalogDataAdapter
from plone.dexterity.interfaces import IDexterityContent
from plone.uuid.interfaces import IUUID
from plone.server.traversal import do_traverse
from plone.server.interfaces import IAbsoluteURL
import asyncio

import logging


logger = logging.getLogger(__name__)


CATALOG_TYPES = {
    'text': {'type': 'string'},
    'int': {'type': 'integer'},
    'date': {'type': 'date'},
    'boolean': {'type': 'boolean'},
    'binary': {'type': 'binary'}
}

INDEX_TYPES = {
    'analyzed': {'index': 'analyzed'},
    'non_analyzed': {'index': 'not_analyzed'}
}


class ElasticSearchUtility(DefaultSearchUtility):

    bulk_size = 50
    initialized = False

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
        for db_name, db in self.app:
            if not IDataBase.providedBy(db):
                continue
            for site_name, site in db:
                try:
                    await self.conn.indices.create(site_name)
                except TransportError:
                    pass
                except ConnectionError:
                    logger.warn('elasticsearch not installed', exc_info=True)
                    return
                except RequestError:
                    return

        # Mapping calculated from schemas
        for name, schema in getUtilitiesFor(IDexterityFTI):
            # For each type
            mappings = {}
            for schema in iterSchemataForType(name):
                # create mapping for content type
                catalog = mergedTaggedValueDict(schema, CATALOG_KEY)
                index = mergedTaggedValueDict(schema, INDEX_KEY)
                for field_name, field in getFields(schema).items():
                    kind_index = index.get(field_name, False)
                    kind_catalog = catalog.get(field_name, False)
                    field_mapping = {}
                    if kind_catalog:
                        if kind_catalog == 'object':
                            # Especial case that is an object
                            # TODO
                            pass
                        field_mapping.update(CATALOG_TYPES[kind_catalog])
                        if kind_index:
                            field_mapping.update(INDEX_TYPES[kind_index])

                        field_name = schema.getName() + '-' + field_name
                        mappings[field_name] = field_mapping
            mappings['accessRoles'] = {
                'type': 'string',
                'index': 'not_analyzed'
            }
            mappings['accessUsers'] = {
                'type': 'string',
                'index': 'not_analyzed'
            }
            mappings['path'] = {
                'type': 'string'
            }
            mappings['uuid'] = {
                'type': 'string',
                'index': 'not_analyzed'
            }
            try:
                await self.conn.indices.put_mapping(
                    "_all", name, body={'properties': mappings})
            except:  # noqa
                logger.warn('elasticsearch not installed', exc_info=True)

        self.initialized = True

    def get_connection(self):
        return Elasticsearch(**self.settings['connection_settings'])

    def reindexContentAndSubcontent(self, obj, loads):
        loads[(IUUID(obj), obj.portal_type)] = ICatalogDataAdapter(obj)()
        if len(loads) == self.bulk_size:
            yield loads
            loads.clear()
        for key, value in obj.items():
            if IDexterityContent.providedBy(value):
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
            if hasattr(request, '_cache_groups'):
                for group in user.principal._groups:
                    roles.extend([key for key, value in request._cache_groups[group]._roles.items() if value])

        if 'filter' in query:
            if 'terms' not in query:
                query['filter']['terms'] = {}
        else:
            if 'terms' not in query:
                query['filter'] = {'terms': {}}
        query['filter']['terms']['accessRoles'] = roles
        # query['filter']['terms']['accessUsers'] = users

        q['body'] = query

        result = await self.conn.search(**q)
        items = []
        site_url = IAbsoluteURL(request.site, request)()
        for item in result['hits']['hits']:
            data = item['_source']
            data.update({
                '@id': site_url + data.get('path', ''),
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
        """
        {uid: <dict>}
        """
        while not self.initialized:
            await asyncio.sleep(1.0)

        if len(datas) > 0:
            bulk_data = []

            for ident, data in datas.items():
                bulk_data.extend([{
                    'index': {
                        '_index': site_id,
                        '_type': ident[1],
                        '_id': ident[0]
                    }
                }, data])
                if len(bulk_data) % self.bulk_size == 0:
                    await self.conn.bulk(
                        index=site_id, doc_type=ident[1],
                        body=bulk_data)
                    bulk_data = []

            if len(bulk_data) > 0:
                await self.conn.bulk(
                    index=site_id, doc_type=ident[1],
                    body=bulk_data)

    async def remove(self, uids, site_id):
        """
        list of UIDs to remove from index
        """
        if len(uids) > 0:
            bulk_data = []
            for uid in uids:
                bulk_data.append({
                    'delete': {
                        '_index': site_id,
                        '_id': uid
                    }
                })
            await self.conn.bulk(
                index=site_id, body=bulk_data)

    async def create_index(self, site_id):
        try:
            await self.conn.indices.create(site_id)
        except TransportError:
            pass
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            return
        except RequestError:
            return

    async def remove_index(self, site_id):
        try:
            await self.conn.indices.delete(site_id)
        except TransportError:
            pass
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            return
        except RequestError:
            return
