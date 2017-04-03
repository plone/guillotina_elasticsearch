from guillotina import configure
from guillotina.component import getUtility
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IContainer


@configure.service(
    context=IContainer, name='@update_mapping', method='POST',
    permission='plone.ManageCatalog')
async def update_mapping(context, request):
    util = getUtility(ICatalogUtility)
    await util.migrate_index(context)
