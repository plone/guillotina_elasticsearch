from guillotina.interfaces import ICatalogUtility


try:
    from guillotina.async_util import IAsyncUtility
except ImportError:
    from guillotina.async import IAsyncUtility


DOC_TYPE = 'doc'

class IElasticSearchUtility(ICatalogUtility, IAsyncUtility):
    pass
