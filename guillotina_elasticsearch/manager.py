# -*- coding: utf-8 -*-
from aioes import Elasticsearch
from aioes.exception import ConnectionError
from aioes.exception import NotFoundError
from aioes.exception import RequestError
from aioes.exception import TransportError
from guillotina import app_settings
from guillotina.catalog.catalog import DefaultSearchUtility
from guillotina.interfaces import IAnnotations
from guillotina.registry import REGISTRY_DATA_KEY
from guillotina.utils import get_current_request
from guillotina_elasticsearch.schema import get_mappings
from guillotina_elasticsearch.utils import safe_es_call

import asyncio
import logging


logger = logging.getLogger('guillotina_elasticsearch')

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
        },
        "filter": {
        },
        "char_filter": {
        }
    },
    'index.mapper.dynamic': False
}


class ElasticSearchManager(DefaultSearchUtility):

    def __init__(self, settings={}, loop=None):
        self.loop = loop
        self._conn = None
        self._migration_lock = None

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
        return len(self.settings.get('connection_settings', {}).get('endpoints', [])) > 0

    async def initialize(self, app):
        self.app = app
        self._migration_lock = asyncio.Lock()

    async def finalize(self, app):
        if self._conn is not None:
            self._conn.close()

    async def get_registry(self, container, request):
        if request is None:
            request = get_current_request()
        if hasattr(request, 'container_settings'):
            return request.container_settings
        annotations_container = IAnnotations(container)
        request.container_settings = await annotations_container.async_get(REGISTRY_DATA_KEY)
        return request.container_settings

    async def get_real_index_name(self, container, request=None):
        index_name = await self.get_index_name(container, request)
        version = await self.get_version(container, request)
        return index_name + '_' + str(version)

    async def get_index_name(self, container, request=None):
        registry = await self.get_registry(container, request)

        try:
            result = registry['el_index_name']
        except KeyError:
            result = app_settings['elasticsearch'].get(
                'index_name_prefix', 'guillotina-') + container.id
        return result

    async def get_next_index_name(self, container, request=None):
        registry = await self.get_registry(container, request)
        if ('el_next_index_version' not in registry or
                registry['el_next_index_version'] is None):
            return None
        index_name = await self.get_index_name(container, request)
        version = registry['el_next_index_version']
        return index_name + '_' + str(version)

    async def set_index_name(self, container, name, request=None):
        registry = await self.get_registry(container, request)
        registry['el_index_name'] = name
        registry._p_register()

    async def initialize_catalog(self, container):
        if not self.enabled:
            return
        await self.remove_catalog(container)
        index_name = await self.get_index_name(container)
        real_index_name = await self.get_real_index_name(container)

        await safe_es_call(self.conn.indices.create, real_index_name)
        await safe_es_call(self.conn.indices.put_alias, index_name, real_index_name)
        await safe_es_call(self.conn.indices.close, index_name)
        await safe_es_call(self.install_mappings_on_index, index_name)

        await self.conn.indices.open(index_name)
        await self.conn.cluster.health(wait_for_status='yellow')
        await self.set_index_name(container, index_name)

    async def remove_catalog(self, container):
        if not self.enabled:
            return
        index_name = await self.get_index_name(container)
        real_index_name = await self.get_real_index_name(container)
        await safe_es_call(self.conn.indices.close, real_index_name)
        await safe_es_call(self.conn.indices.delete_alias, real_index_name, index_name)
        await safe_es_call(self.conn.indices.delete, real_index_name)
        await safe_es_call(self.conn.indices.delete, index_name)

    async def get_version(self, container, request=None):
        registry = await self.get_registry(container, request)
        try:
            version = registry['el_index_version']
        except KeyError:
            version = 1
        return version

    async def set_version(self, container, version, request=None, force=False):
        registry = await self.get_registry(container, request)
        if (not force and 'el_next_index_version' in registry and
                registry['el_next_index_version'] is not None):
            raise Exception('Cannot change index while migration is in progress')
        registry['el_index_version'] = version
        registry._p_register()

    async def stats(self, container):
        index_name = await self.get_index_name(container)
        return await self.conn.indices.stats(index_name)

    async def install_mappings_on_index(self, index_name):
        mappings = get_mappings()
        index_settings = DEFAULT_SETTINGS.copy()
        index_settings.update(app_settings.get('index', {}))
        await self.conn.indices.close(index_name)
        await self.conn.indices.put_settings(index_settings, index_name)
        for key, value in mappings.items():
            await self.conn.indices.put_mapping(index_name, key, value)
        await self.conn.indices.open(index_name)

    async def activate_next_index(self, container, version, request=None, force=False):
        '''
        Next index support designates an index to also push
        delete and index calls to
        '''
        registry = await self.get_registry(container, request)
        if not force:
            try:
                assert registry['el_next_index_version'] is None
            except KeyError:
                pass
        registry['el_next_index_version'] = version
        registry._p_register()

    async def disable_next_index(self, container, request=None):
        '''
        Next index support designates an index to also push
        delete and index calls to
        '''
        registry = await self.get_registry(container, request)
        registry['el_next_index_version'] = None
        registry._p_register()

    async def apply_next_index(self, container, request=None):
        # make sure to reload the registry to make sure we have the latest
        # to write to
        if (request is not None and hasattr(request, 'container_settings') and
                REGISTRY_DATA_KEY in container.__annotations__):
            await request._txn.refresh(request.container_settings)
        registry = await self.get_registry(container, request)
        assert registry['el_next_index_version'] is not None
        await self.set_version(
            container, registry['el_next_index_version'], request, force=True)
        registry['el_next_index_version'] = None
        registry._p_register()
