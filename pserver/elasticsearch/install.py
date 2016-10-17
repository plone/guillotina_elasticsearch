# -*- coding: utf-8 -*-
from plone.registry.interfaces import IRegistry
from plone.server.addons import Addon
from plone.server.registry import ILayers
from zope.component import getUtility


ELASTICSEARCH_LAYER = 'pserver.elasticsearch.interfaces.IElasticSearchLayer'


class ElasticSearchAddon(Addon):

    def install(self, site):
        registry = getUtility(IRegistry)
        registry.forInterface(ILayers).active_layers.append(
            ELASTICSEARCH_LAYER
        )

    def uninstall(self):
        registry = getUtility(IRegistry)
        registry.forInterface(ILayers).active_layers.remove(
            ELASTICSEARCH_LAYER
        )
