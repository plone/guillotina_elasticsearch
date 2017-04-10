.. contents::

GUILLOTINA_ELASTICSEARCH
========================


Configuration
-------------

config.json can include elasticsearch section::

    "elasticsearch": {
        "index_name_prefix": "guillotina-",
        "connection_settings": {
            "endpoints": ["localhost:9200"],
            "sniffer_timeout": 0.5
        }
    }


Installation on a site
----------------------

POST SITE_URL/@catalog

{}

Uninstall on a site
-------------------

DELETE SITE_URL/@catalog

{}
