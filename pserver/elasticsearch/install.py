# -*- coding: utf-8 -*-
from plone.server.addons import Addon
from plone.server.registry import ILayers


ELASTICSEARCH_LAYER = 'pserver.elasticsearch.interfaces.IElasticSearchLayer'


class ElasticSearchAddon(Addon):

    @classmethod
    def install(self, request):
        registry = request.site_settings
        registry.forInterface(ILayers).active_layers.append(
            ELASTICSEARCH_LAYER
        )

    @classmethod
    def uninstall(self, request):
        registry = request.site_settings
        registry.forInterface(ILayers).active_layers.remove(
            ELASTICSEARCH_LAYER
        )
