from guillotina import configure
from guillotina.component import getUtility
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IContainer
from guillotina.component import queryUtility
from guillotina_elasticsearch.manager import get_mappings
from guillotina_elasticsearch.manager import DEFAULT_SETTINGS
from guillotina import app_settings


@configure.service(
    context=IContainer, name='@update_mapping', method='POST',
    permission='guillotina.ManageCatalog')
async def update_mapping(context, request):
    util = getUtility(ICatalogUtility)
    await util.migrate_index(context)


@configure.service(
    context=IContainer, name='@force_mapping', method='POST',
    permission='guillotina.ManageCatalog')
async def force_update_mapping(context, request):
    catalog = queryUtility(ICatalogUtility)
    index_name = await catalog.get_index_name(request.container)
    mappings = get_mappings()
    index_settings = DEFAULT_SETTINGS.copy()
    index_settings.update(app_settings.get('index', {}))

    await catalog.conn.indices.close(index_name)
    await catalog.conn.indices.put_settings(
        index_settings, index_name)
    await catalog.conn.indices.open(index_name)
    for key, value in mappings.items():
        await catalog.conn.indices.put_mapping(index_name, key, value)
    return {
        'status': 'ok'
    }
