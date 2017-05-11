from guillotina import configure
from guillotina.component import getUtility
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IContainer
from guillotina.component import queryUtility
from guillotina_elasticsearch.manager import get_mappings


@configure.service(
    context=IContainer, name='@update_mapping', method='POST',
    permission='plone.ManageCatalog')
async def update_mapping(context, request):
    util = getUtility(ICatalogUtility)
    await util.migrate_index(context)


@configure.service(
    context=IContainer, name='@force_mapping', method='POST',
    permission='plone.ManageCatalog')
async def update_mapping(context, request):
    catalog = queryUtility(ICatalogUtility)
    index_name = await catalog.get_index_name(request.container)
    mappings = get_mappings()
    for key, value in mappings.items():
        await catalog.conn.indices.put_mapping(index_name, key, value)
    return {
        'status': 'ok'
    }
