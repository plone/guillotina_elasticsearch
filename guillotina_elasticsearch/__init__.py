# -*- coding: utf-8 -*-
from guillotina import configure
from guillotina.catalog.utils import get_index_fields
from guillotina.component import get_utilities_for
from guillotina.content import IResourceFactory
from guillotina.utils import get_dotted_name


app_settings = {
    "elasticsearch": {
        "bulk_size": 50,
        "index_name_prefix": "guillotina-",
        "connection_settings": {
            "hosts": [],
            "timeout": 2
        },
        "index": {}
    },
    'commands': {
        'es-migrate': 'guillotina_elasticsearch.commands.migrate.MigrateCommand',
        'es-reindex': 'guillotina_elasticsearch.commands.reindex.ReindexCommand',
        'es-vacuum': 'guillotina_elasticsearch.commands.vacuum.VacuumCommand',
        'es-fields': 'guillotina_elasticsearch.commands.fields.FieldsCommand'
    }
}


def includeme(root):
    configure.scan('guillotina_elasticsearch.utility')
    configure.scan('guillotina_elasticsearch.manager')

    # add store true to guillotina indexes
    for name, utility in get_utilities_for(IResourceFactory):
        if not get_dotted_name(utility._callable).startswith('guillotina.'):
            continue
        for field_name, catalog_info in get_index_fields(name).items():
            if field_name in ('id', 'path', 'uuid', 'type_name', 'tid',
                              'creators', 'contributors', 'access_roles',
                              'access_users', 'parent_uuid', 'title'):
                catalog_info['store'] = True
