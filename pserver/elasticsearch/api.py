from plone.server import configure
from zope.component import getUtility
from plone.server.interfaces import ICatalogUtility
from plone.server.interfaces import ISite


@configure.service(
    context=ISite, name='@update_mapping', method='POST',
    permission='plone.ManageCatalog')
async def update_mapping(context, request):
    util = getUtility(ICatalogUtility)
    await util.migrate_index(context)
