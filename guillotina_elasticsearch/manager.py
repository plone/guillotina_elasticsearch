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

import json
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
        }
    },
    'index.mapper.dynamic': False
}


class ElasticSearchManager(DefaultSearchUtility):

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
        return len(self.settings.get('connection_settings', {}).get('endpoints', [])) > 0

    async def initialize(self, app):
        self.app = app

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

    async def get_index_name(self, container, request=None):
        if request is not None and hasattr(request, '_cache_index_name'):
            return request._cache_index_name
        registry = await self.get_registry(container, request)

        try:
            result = registry['el_index_name']
        except KeyError:
            result = app_settings['elasticsearch'].get(
                'index_name_prefix', 'guillotina-') + container.id
        if request is not None:
            request._cache_index_name = result
        return result

    async def set_index_name(self, container, name, request=None):
        if hasattr(request, '_cache_index_name'):
            request._cache_index_name = name
        registry = await self.get_registry(container, request)
        registry['el_index_name'] = name
        registry._p_register()

    async def initialize_catalog(self, container):
        if not self.enabled:
            return
        await self.remove_catalog(container)
        mappings = get_mappings()
        index_name = await self.get_index_name(container)
        version = await self.get_version(container)
        real_index_name = index_name + '_' + str(version)
        index_settings = DEFAULT_SETTINGS.copy()
        index_settings.update(app_settings.get('index', {}))
        try:
            await self.conn.indices.create(real_index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass

        try:
            await self.conn.indices.put_alias(index_name, real_index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass

        try:
            await self.conn.indices.close(index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass

        try:
            await self.conn.indices.put_settings(index_settings, index_name)
            for key, value in mappings.items():
                await self.conn.indices.put_mapping(index_name, key, value)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            return
        except RequestError:
            return
        await self.conn.indices.open(index_name)
        await self.conn.cluster.health(wait_for_status='yellow')
        await self.set_index_name(container, index_name)

    async def remove_catalog(self, container):
        if not self.enabled:
            return
        index_name = await self.get_index_name(container)
        version = await self.get_version(container)
        real_index_name = index_name + '_' + str(version)
        try:
            await self.conn.indices.close(real_index_name)
        except NotFoundError:
            pass
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except (RequestError, RuntimeError):
            pass

        try:
            await self.conn.indices.delete_alias(real_index_name, index_name)
        except NotFoundError:
            pass
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except (ConnectionError, RuntimeError):
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass

        try:
            await self.conn.indices.delete(real_index_name)
        except NotFoundError:
            pass
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except (ConnectionError, RuntimeError):
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass

        try:
            await self.conn.indices.delete(index_name)
        except NotFoundError:
            pass
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except (ConnectionError, RuntimeError):
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except (RequestError, NotFoundError):
            pass

    async def get_version(self, container):
        registry = await self.get_registry(container, None)
        try:
            version = registry['el_index_version']
        except KeyError:
            version = 1
        return version

    async def set_version(self, container, version):
        registry = await self.get_registry(container, None)
        registry['el_index_version'] = version
        registry._p_register()

    async def stats(self, container):
        index_name = await self.get_index_name(container)
        return await self.conn.indices.stats(index_name)

    async def migrate_index(self, container):
        index_name = await self.get_index_name(container)
        version = await self.get_version(container)
        mappings = get_mappings()
        index_settings = DEFAULT_SETTINGS.copy()
        index_settings.update(app_settings.get('index', {}))
        next_version = version + 1
        real_index_name = index_name + '_' + str(version)
        real_index_name_next_version = index_name + '_' + str(next_version)
        temp_index = index_name + '_' + str(next_version) + '_t'

        # Create and setup the new index
        exists = await self.conn.indices.exists(index_name)
        if exists:
            logger.warn('Canonical index exist')
            await self.conn.indices.delete(index_name)

        # Create and setup the new index
        exists = await self.conn.indices.exists(real_index_name_next_version)
        if exists:
            logger.warn('New version exist')
            await self.conn.indices.delete(real_index_name_next_version)

        exists = await self.conn.indices.exists(temp_index)
        conn_es = await self.conn.transport.get_connection()

        if exists:
            # There is a temp index so it needs to be reindex to the old one
            # Its been a failing reindexing
            logger.warn('Temp index exist')
            # Move aliases
            body = {
                "actions": [
                    {"remove": {
                        "alias": index_name,
                        "index": temp_index
                    }},
                    {"add": {
                        "alias": index_name,
                        "index": real_index_name
                    }}
                ]
            }
            conn_es = await self.conn.transport.get_connection()
            async with conn_es._session.post(
                        conn_es._base_url + '_aliases',
                        data=json.dumps(body),
                        timeout=1000000
                    ) as resp:
                pass
            body = {
              "source": {
                "index": temp_index
              },
              "dest": {
                "index": real_index_name
              }
            }
            async with conn_es._session.post(
                        conn_es._base_url + '_reindex',
                        data=json.dumps(body)
                    ) as resp:
                pass
            await self.conn.indices.delete(temp_index)

        await self.conn.indices.create(temp_index)
        await self.conn.indices.create(real_index_name_next_version)
        await self.conn.indices.close(real_index_name_next_version)
        await self.conn.indices.put_settings(
            index_settings, real_index_name_next_version)
        for key, value in mappings.items():
            await self.conn.indices.put_mapping(
                real_index_name_next_version, key, value)
        await self.conn.indices.open(real_index_name_next_version)

        # Start to duplicate aliases
        body = {
            "actions": [
                {"remove": {
                    "alias": index_name,
                    "index": real_index_name
                }},
                {"add": {
                    "alias": index_name,
                    "index": temp_index
                }}
            ]
        }

        async with conn_es._session.post(
                    conn_es._base_url + '_aliases',
                    data=json.dumps(body),
                    timeout=1000000
                ) as resp:
            pass
        logger.warn('Updated aliases')

        # Reindex
        body = {
          "source": {
            "index": real_index_name
          },
          "dest": {
            "index": real_index_name_next_version
          }
        }
        conn_es = await self.conn.transport.get_connection()
        async with conn_es._session.post(
                    conn_es._base_url + '_reindex',
                    data=json.dumps(body),
                    timeout=10000000
                ) as resp:
            pass
        logger.warn('Reindexed')

        # Move aliases
        body = {
            "actions": [
                {"remove": {
                    "alias": index_name,
                    "index": temp_index
                }},
                {"add": {
                    "alias": index_name,
                    "index": real_index_name_next_version
                }}
            ]
        }
        conn_es = await self.conn.transport.get_connection()
        async with conn_es._session.post(
                    conn_es._base_url + '_aliases',
                    data=json.dumps(body),
                    timeout=1000000
                ) as resp:
            pass
        logger.warn('Updated aliases')
        await self.set_version(container, next_version)

        # Reindex
        body = {
          "source": {
            "index": temp_index
          },
          "dest": {
            "index": real_index_name_next_version
          }
        }
        async with conn_es._session.post(
                    conn_es._base_url + '_reindex',
                    data=json.dumps(body)
                ) as resp:  # noqa
            pass
        logger.warn('Reindexed temp')

        # Delete old index
        await self.conn.indices.close(real_index_name)
        await self.conn.indices.delete(real_index_name)
        await self.conn.indices.close(temp_index)
        await self.conn.indices.delete(temp_index)
