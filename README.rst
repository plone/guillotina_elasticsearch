.. contents::

GUILLOTINA_ELASTICSEARCH
========================

.. image:: https://travis-ci.org/guillotinaweb/guillotina_elasticsearch.svg?branch=master
   :target: https://travis-ci.org/guillotinaweb/guillotina_elasticsearch

Elasticsearch integration for guillotina.


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


Commands
--------

`guillotina_elasticsearch` comes with a `es-reindex` guillotina command::

    ./bin/g es-reindex



Improvements
------------

- live reindex
  - reindex on new index, switch over when done
- optimized reindex
  - run updates on diff of indexes that have changed
- after reindex finishes, identify
  - orphaned docs
  - missing docs
