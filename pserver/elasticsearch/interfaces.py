# -*- encoding: utf-8 -*-
from zope.interface import Interface
from plone.server.catalog.interfaces import ICatalogUtility


class IElasticSearchLayer(Interface):
    """Marker interface layer Elastic Search."""


class IElasticSearchUtility(ICatalogUtility):
    """Configuration utility.
    """
