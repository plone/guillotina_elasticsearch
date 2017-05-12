from guillotina import configure
from guillotina.component import getUtility
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IContainer
from guillotina.component import queryUtility
from guillotina_elasticsearch.manager import get_mappings
from guillotina_elasticsearch.manager import DEFAULT_SETTINGS
from guillotina import app_settings
import json


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
    version = await catalog.get_version(request.container)
    real_index_name = index_name + '_' + str(version)
    mappings = get_mappings()
    index_settings = DEFAULT_SETTINGS.copy()
    index_settings.update(app_settings.get('index', {}))

    await catalog.conn.indices.close(real_index_name)
    await catalog.conn.indices.put_settings(
        index_settings, real_index_name)
    await catalog.conn.indices.open(real_index_name)
    conn_es = await catalog.conn.transport.get_connection()
    response = {
        'status': 200
    }
    for key, value in mappings.items():
        async with conn_es._session.put(
                    str(conn_es._base_url) + '_mapping/' + key + '?update_all_types',
                    data=json.dumps(value),
                    timeout=1000000
                ) as resp:
            if resp.status != 200:
                response = {
                    'status': resp.status,
                    'error': await resp.text()
                }

    return response
