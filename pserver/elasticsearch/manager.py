# -*- coding: utf-8 -*-
from aioes import Elasticsearch
from aioes.exception import ConnectionError
from aioes.exception import RequestError
from aioes.exception import TransportError
from plone.server import app_settings
from plone.server.catalog.catalog import DefaultSearchUtility
from pserver.elasticsearch.schema import get_mappings

import logging
import json

logger = logging.getLogger('pserver.elasticsearch')

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
    }
}


class ElasticSearchManager(DefaultSearchUtility):

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
            return app_settings['elasticsearch'].get(
                'index_name_prefix', 'plone-') + site.id

    def set_index_name(self, site, name):
        site['_registry']['el_index_name'] = name

    async def initialize_catalog(self, site):
        await self.remove_catalog(site)
        mappings = get_mappings()
        index_name = self.get_index_name(site)
        version = self.get_version(site)
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
        self.set_index_name(site, index_name)

    async def remove_catalog(self, site):
        index_name = self.get_index_name(site)
        version = self.get_version(site)
        real_index_name = index_name + '_' + str(version)
        try:
            await self.conn.indices.close(real_index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass
        
        try:
            await self.conn.indices.delete_alias(real_index_name, index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass

        try:
            await self.conn.indices.delete(real_index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass

        try:
            await self.conn.indices.delete(index_name)
        except TransportError as e:
            logger.warn('Transport Error', exc_info=e)
        except ConnectionError:
            logger.warn('elasticsearch not installed', exc_info=True)
            pass
        except RequestError:
            pass

    def get_version(self, site):
        try:
            version = site['_registry']['el_index_version']
        except KeyError:
            version = 1
        return version

    def set_version(self, site, version):
        site['_registry']['el_index_version'] = version

    async def stats(self, site):
        index_name = self.get_index_name(site)
        self.conn.indices.stats(index_name)

    async def migrate_index(self, site):
        index_name = self.get_index_name(site)
        version = self.get_version(site)
        mappings = get_mappings()
        index_settings = DEFAULT_SETTINGS.copy()
        index_settings.update(app_settings.get('index', {}))
        next_version = version + 1
        real_index_name = index_name + '_' + str(version)
        real_index_name_next_version = index_name + '_' + str(next_version)
        temp_index = index_name + '_' + str(next_version) + '_t'

        # Create and setup the new index
        exists = await self.conn.indices.exists(real_index_name_next_version)
        if exists:
            await self.conn.indices.delete(real_index_name_next_version)

        exists = await self.conn.indices.exists(temp_index)
        if exists:
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
        await self.conn.indices.put_alias(index_name, temp_index)

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
                    data=json.dumps(body)
                ) as resp:
            pass
        logger.warn('Reindexed')
        
        # Move aliases
        body = {
            "actions": [
                {"remove": {
                    "alias": index_name,
                    "index": real_index_name
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
                    data=json.dumps(body)
                ) as resp:
            pass
        logger.warn('Updated aliases')
        self.set_version(site, next_version)

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
                ) as resp:
            pass
        logger.warn('Reindexed temp')

        # Delete old index
        await self.conn.indices.close(real_index_name)
        await self.conn.indices.delete(real_index_name)
        await self.conn.indices.close(temp_index)
        await self.conn.indices.delete(temp_index)
