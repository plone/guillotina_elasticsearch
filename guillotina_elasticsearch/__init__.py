# -*- coding: utf-8 -*-
from guillotina import configure


app_settings = {
    "elasticsearch": {
        "bulk_size": 50,
        "index_name_prefix": "guillotina-",
        "connection_settings": {
            "endpoints": [],
            "sniffer_timeout": 0.5
        },
        "index": {},
        "mapping_overrides": {
            "*": {}
        }
    },
    'commands': {
        'es-migrate': 'guillotina_elasticsearch.commands.migrate.MigrateCommand',
        'es-reindex': 'guillotina_elasticsearch.commands.reindex.ReindexCommand',
        'es-vacuum': 'guillotina_elasticsearch.commands.vacuum.VacuumCommand'
    }
}


def includeme(root):
    configure.scan('guillotina_elasticsearch.utility')
