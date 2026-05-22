.. contents::

GUILLOTINA_ELASTICSEARCH
========================

.. image:: https://travis-ci.org/guillotinaweb/guillotina_elasticsearch.svg?branch=master
   :target: https://travis-ci.org/guillotinaweb/guillotina_elasticsearch

Elasticsearch integration for guillotina. Supports Elastic search 7.x,
8.x and 9.x.


Installation
------------

`pip install guillotina_elasticsearch` installs the Elasticsearch 9.x
Python client and supports Elasticsearch clusters 7.x, 8.x and 9.x.


Compatibility Matrix
--------------------

.. list-table::
   :header-rows: 1

   * - guillotina_elasticsearch
     - Python Elasticsearch client
     - ES 6.x
     - ES 7.x
     - ES 8.x
     - ES 9.x
   * - 6.x
     - aioelasticsearch 0.5/0.6
     - Yes *
     - Yes
     - No
     - No
   * - 7.x
     - elasticsearch-py 7.x
     - No
     - Yes
     - No
     - No
   * - 8.x
     - elasticsearch-py 8.x
     - No
     - Yes
     - Yes
     - No
   * - 8.0.19+
     - elasticsearch-py 9.x
     - No
     - Yes
     - Yes
     - Yes

* Elasticsearch 6.x support in the 6.x package line required pinning
  ``aioelasticsearch<0.6.0``. Current releases do not support Elasticsearch
  6.x clusters.

This package uses Elasticsearch compatibility headers so the 9.x Python
client can communicate with supported Elasticsearch 7.x and 8.x clusters.


Configuration
-------------

config.yaml can include elasticsearch section

.. code-block:: yaml

    elasticsearch:
      index_name_prefix: "guillotina-"
      connection_settings:
        hosts:
          - "http://127.0.0.1:9200"
        sniffer_timeout: 0.5
        sniff_on_start: true
      security_query_builder: "guillotina_elasticsearch.queries.build_security_query"


Example custom `security_query_builder` settings:

.. code-block:: python

    async def security_query_builder(container, request):
        return {
            'query': {
                'bool': {
                    'filter': {
                        'bool': {
                            'should': [{'match': {'access_roles': "foobar"}}],
                            'minimum_should_match': 1
                        }
                    }
                }
            }
        }

Development and testing
-----------------------
Setup your python virtual environment for version >=3.10.

.. code-block:: bash

   # Linux
   pip install -e ".[test]"
   ES_TEST_VERSION=7 pytest guillotina_elasticsearch/tests
   ES_TEST_VERSION=8 pytest guillotina_elasticsearch/tests
   ES_TEST_VERSION=9 pytest guillotina_elasticsearch/tests

By default the tests run an ES fixture with version 9. Use
`ES_TEST_VERSION` to select a supported Elasticsearch cluster. The fixture
also recognizes `ES_TEST_VERSION=6` for older release-line checks, but this
branch does not support Elasticsearch 6.


Installation on a site
----------------------

POST SITE_URL/@catalog

{}

Uninstall on a site
-------------------

DELETE SITE_URL/@catalog

{}


Migrate Command
---------------

`guillotina_elasticsearch` comes with a `es-migrate` guillotina command::

    ./bin/g es-migrate


This command will perform a live migration on the index. It does this by
performing the reindex on a new index while the other one is still active.

New index and delete requests are performed on both indexes during live migration.

It is also smart about how to migrate, doing a diff on the mapping and only
reindexing the fields that changed.

Breaking changes in 8.0.19
--------------------------

- Python 3.10 or newer is required.
- Guillotina 7.0.0 or newer is required.
- The Python Elasticsearch client dependency is now 9.x
  (`elasticsearch[async]>=9.0.0,<10.0.0`).
- Elasticsearch clusters 7.x, 8.x and 9.x are supported through the
  Elasticsearch compatibility headers used by the connection factory.


Breaking changes in 8.0.0
-------------------------

In this version, the library elasticsearch 7 has been upgraded to
elasticsearch 8. There are some changes that need to be taken into
account in the settings of old elasticsearch config files.

- The hosts field in the guillotina's configuration file, need to
  include the scheme: http or https
- The sniffer_timeout in the guillotina's configureation file, can not be None
- The doc_type has been removed. Specifying types in requests is no longer supported.
- The include_type_name parameter is removed.

The elasticsearch field of the config.yaml file is directly passed to
instantiate AsyncElasticsearch. The class definition is the same of
the synchronous one, to know how to configure your ES take a look at:
https://elasticsearch-py.readthedocs.io/en/v8.12.0/api/elasticsearch.html#elasticsearch.Elasticsearch


Breaking changes in 2.0
-----------------------

- ES 6 does not have doc types support
- aioes deprecated
- IElasticSearchUtility changes:

  - query: doc_type param no longer used

- IElasticSearchUtility.conn changes:

  - put_mapping
  - put_settings
  - put_alias
  - get: needs doc_type=DOC_TYPE
  - bulk: needs doc_type=DOC_TYPE
  - conn.transport.get_connection(): ._session -> .session, ._base_url -> .base_url
  - conn.transport.get_connection().[method] -> need to include content-type: application/json


Testing
-------

If container es (elasticsearch) fails to start when running tests,
you should increase max_map_count. command

.. code-block:: bash

   # Linux
   sudo sysctl -w vm.max_map_count=262144


Index Mappings
--------------

All mappings are flattened when they are applied to Elastic Search.

Elasticsearch doesn't support different mappings for different types.

If there are conflicting mappings, this plugin will choose the mapping
in the addon last `applications` listed which follows the guillotina practice
of allowing the last listed application to override the settings of
the previously list application


Security configuration
----------------------


.. code-block:: yaml

    elasticsearch:
      index_name_prefix: "guillotina-"
      connection_settings:
        hosts:
          - "127.0.0.1:9200"
        sniffer_timeout: 0.5
        sniff_on_start: true
        use_ssl: true
        http_auth: admin:admin
