# -*- coding: utf-8 -*-

app_settings = {
    "elasticsearch": {
        "bulk_size": 50,
        "index_name_prefix": "plone-",
        "connection_settings": {
            "endpoints": [],
            "sniffer_timeout": 0.5
        },
        "index": {},
        "mapping_overrides": {
            "*": {}
        }
    }
}


utility_config = {
    "provides": "plone.server.interfaces.ICatalogUtility",
    "factory": "pserver.elasticsearch.utility.ElasticSearchUtility",
    "settings": {}  # all settings are on the global object
}


# def includeme(root):
#     root.add_async_utility(utility_config)
