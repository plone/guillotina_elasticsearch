8.0.13 (2025-07-30)
-------------------

- Adding count functionality, working along with the @count endpoint
  of guillotina
  [nilbacardit26]
- Fixing __or query when using null values in fields
  [nilbacardit26]


8.0.12 (2025-02-25)
-------------------

- Getting the real count of documents if items_total is equal 10000
  [nilbacardit26]


8.0.11 (2025-02-25)
-------------------

- Casting max_result_window_value to int inside search_raw
  [nilbacardit26]


8.0.10 (2025-01-23)
-------------------

- Adding {field}__not query param without value to search documents that do
  not contain the field
  [nilbacardit26]

8.0.9 (2024-10-21)
------------------

- Avoiding possible error when getting the index_settings in
  search_raw
  [nilbacardit26]


8.0.8 (2024-10-17)
------------------

- Do not raise an error when the real_index_name is not synchronize
  with the registry when doing a search and calculating the
  max_result_window_value
  [nilbacardit26]


8.0.7 (2024-10-17)
------------------

- Getting items_total from conn.count instead of getting it from the
  hits of the result
  [nilbacardit26]


8.0.6 (2024-10-01)
------------------

- Log results every time data is bulk_inserted when updating
  [nilbacardit26]


8.0.5 (2024-10-01)
------------------

- Do not log results if there are none to show, preventing errors
  [nilbacardit26]


8.0.4 (2024-07-19)
------------------

- Adding the reason of the error when migrating
- Adding query parameter to the get_doc_count coroutine 
  [nilbacardit26]


8.0.3 (2024-04-10)
------------------

- Adding search analyzers functionality
  [nilbacardit26]


8.0.2 (2024-03-25)
------------------

- Being able to search using multifields
  [nilbacardit26]


8.0.1 (2024-03-21)
------------------

- Adding missing index kwarg when deleting the index in the middle
  of a migration
  [nilbacardit26]


8.0.0 (2024-01-23)
------------------

- Support for elasticsearch 8.
- Changing ES fixture to bitnami version 8. Tests passing for both
  versions
- Removing deprecated doc_type argument from get calls.
- Dropping support for elastic search 6.x
  [nilbacardit26]


7.0.5 (2023-12-20)
------------------

- Fixing applications in __init__.py: Removing
  guillotina_elasticsearch from it


7.0.4 (2023-12-15)
------------------

- Not adding a slash in the __start modifier of the parser if it is
  already included from the base parser.


7.0.3 (2023-02-08)
------------------

- Adding multifields functionality, mutlifields param can be passed to
  index_field. Useful when wanting to index the same field in
  different ways


7.0.2 (2022-11-23)
------------------

- Being able to build mappings properties with normalizers
  [nilbacardit26]


7.0.1 (2022-10-28)
------------------

- Fix potential indexing bug when full=True and reindex_security=True
  [masipcat]

- Being able to build mappings properties with analyzers
  [nilbacardit26]


7.0.0 (2022-03-16)
------------------

- Adapt guillotina_elasticserach code to guillotina 6.4 breaking changes
  [masipcat]


7.0.0a7 (2021-09-14)
--------------------

- Fix guillotina_elasticserach 7.0.0a6 doesn't load catalog automatically as before
  [masipcat]


7.0.0a6 (2021-09-13)
--------------------

- [BREAKING CHANGE] Remove 'sub indexes' logic
  [masipcat]
- Minor changes to ElasticSearchUtility
  [masipcat]
- Remove deprecated parameter 'doc_type'
  [masipcat]
- ElasticSearchUtility: removed obsolete methods get_by_uuid(), get_by_uuids(),
  get_by_type() and get_by_type()
  [masipcat]
- ElasticSearchUtility: removed unused internal method _get_type_query()
  [masipcat]
- Solving bug when searching within the same context that starts by
  the same path. ej: /container/folder/type_one and /container/folder/type.
  [nilbacardit26]
- Enlarging the max time to wait for the ES docker container. Useful
  when passing test with xdist with multiple threads.
  [nilbacardit26]


7.0.0a5 (2021-07-30)
--------------------

- Modifying the parse to be able to parse the __or field as a list of elements. Eg:
  catalog_utility.search({"type_name__or": ["Folder", "Item"]})
  [nilbacardit26]


7.0.0a4 (2021-07-15)
--------------------

- Adding path as a wildcard in the parser. Searches will be done
  within the same context using the endpoint @search. FYI: If no depth is
  specified, the query resolves greater or equal than the content depth of the context plus one
  [nilbacardit26]


7.0.0a3 (2021-06-22)
--------------------

- Fixing the date parser. Using the default format
  strict_date_optional_time instead of using the epoch_millis that
  leads to the error: failed to parse date field [1.624173663E9].


7.0.0a2 (2021-06-09)
--------------------

- Added __or in parser. With this we can query keywords with the or
  operator like: `type_name=Item&__or=id=foo_item%26id=foo_item2`
  [nilbacardit26]


7.0.0a1 (2021-06-04)
--------------------

- Unrestricted parameter added to search_raw, in order to search as root
  [nilbacardit26]


7.0.0a0 (2021-05-03)
--------------------

- Replace aioelasticsearch with elasticsearchpy
  [masipcat]

- Dropped support for Elasticsearch 6.x
  [masipcat]


6.0.3 (2021-04-30)
------------------

- Disable dynamic mapping (as it was in v3.x)
  [masipcat]

- Fix tm is None when migration is cancelled
  [masipcat]


6.0.2 (2021-04-21)
------------------

- Allow to search on context object
  [bloodbare]

- Fix tests not passing
  [masipcat]


6.0.1 (2021-03-02)
------------------

- Replacing _from to from in Parser's __call__ [nilbacardit26]


6.0.0 (2020-07-10)
------------------

- Changed es_requester fixture to only cleanup indices that match 'index_name_prefix'


6.0.0a2 (2020-05-12)
--------------------

- Support es6 and es7 [lferran]

- Add github actions and remove travis [lferran]

- Add setting 'refresh'
  [masipcat]

- Check code format
  [acatlla]


6.0.0a1 (2020-03-25)
--------------------

- Move default settings definition to function `default_settings()`
  [masipcat]


6.0.0a0 (2020-03-12)
--------------------

- Support Guillotina 6
  [masipcat]

- Support elasticsearch 7.0
  [jordic]

- Make sure to save sub index changes in ES
  [vangheem]

- Fix default index settings
  [vangheem]

- Pinned aioelasticsearch to <0.6.0
  [masipcat]

- Be able to import types
  [vangheem]

- Retry conflict errors on delete by query

- Pay attention to trashed objects in pg
- Fix commands using missing attribute `self.request`

- ISecurityInfo can be async

- Fix not iterating over all content indexes in elasticsearch
  [vangheem]

- build_security_query(): changed 'query.bool.filter' to use a list instead of a single object
  [masipcat]

- Fix release

- Missing pg conn lock with vacuuming
  [vangheem]

- Pass request on the index progress when possible

- Fix release

- Do not require request object for vacuuming
  [vangheem]

- G5 support
  [vangheem]

- Do not close indexes on create/delete
  [vangheem]

- Handle another index not found error on vacuum
  [vangheem]

- logging
  [vangheem]

- Handle index not found error
  [vangheem]


5.0.0 (2019-10-21)
------------------

- final


5.0.0a3 (2019-10-21)
--------------------

- more G5 fixes


5.0.0a2 (2019-06-21)
--------------------

- Add parser to work with g5 automatic parsing

- Use `@id` in results results instead of `@absolute_url`


5.0.0a1 (2019-06-19)
--------------------

- Support only elasticsearch 7

- We may want to pin newest version of aioelasticsearch when that also
  supports ES7: https://github.com/aio-libs/aioelasticsearch/pull/165

- Check supported ES version on utility initialize

- Validate index name does not have ':' characters

[lferran]

3.4.0 (2019-05-28)
------------------

- Support for elasticsearch 7
  [bloodbare]


3.3.1 (2019-05-20)
------------------

- Handle `ModuleNotFoundError` error in vacuum
  [vangheem]


3.3.0 (2019-04-10)
------------------

- Be able to customize how you create es connection object
  [vangheem]

- Not loading utility by default and avoid crash when utility is not configured
  [bloodbare]

- Support for opendistro
  [vangheem]


3.2.6 (2019-02-18)
------------------

- Make sure to use transaction lock in vacuum
  [vangheem]

3.2.5 (2019-02-08)
------------------

- be able to customize security query by customizing
  `elasticsearch["security_query_builder"]` setting.
  [vangheem]


3.2.4 (2019-02-05)
------------------

- include highlight in results
  [vangheem]


3.2.3 (2019-01-31)
------------------

- Add 'creation_date', 'modification_date', 'tags' to stored fields
  [vangheem]

- Log indexing errors
  [vangheem]


3.2.2 (2019-01-26)
------------------

- Resolve mapping conflicts in a smarter way that allows addons
  to override base packages/guillotina
  [vangheem]

3.2.1 (2019-01-25)
------------------

- vacuum should work with customized pg table names
  [vangheem]

- Use cursor for iterating over data
  [vangheem]


3.2.0 (2018-12-12)
------------------

- Add kw argument `cache=True` to `Migrator` and `Reindexer` to choose
  overwrite the txn._cache or not [masipcat]
- Prevent more than one index_field define different mappings
  [masipcat]
- Remove use of clear_conn_statement_cache
  [vangheem]


3.1.0 (2018-11-20)
------------------

- upgrade to guillotina >= 4.3.0
  [vangheem]

- Fix indexing when object does not yet exist in index
  [vangheem]

- Fix tid check in vacuum
  [vangheem]


3.0.26 (2018-07-19)
-------------------

- The guillotina.async import is a syntax error on python 3.7
  [vangheem]

- Don't fail on CREATE_INDEX


3.0.25 (2018-06-18)
-------------------

- Do not reprocess if tid is not present in data
  [vangheem]

- retry conflict errors and thread pool exceeded errors
  [vangheem]


3.0.24 (2018-06-13)
-------------------

- add pg index to make vacuuming faster
  [vangheem]


3.0.23 (2018-06-11)
-------------------

- Handle TypeError when vacuuming
  [vangheem]


3.0.22 (2018-06-08)
-------------------

- Upgrade vacuum command to work with moved content
  that wasn't reindexed correctly
  [vangheem]


3.0.21 (2018-06-07)
-------------------

- Fix compatibility with guillotina 4
  [vangheem]


3.0.20 (2018-05-31)
-------------------

- Fix last


3.0.19 (2018-05-31)
-------------------

- utilize ignore_unavailable for elasticsearch queries
  [vangheem]


3.0.18 (2018-05-30)
-------------------

- Also handle ModuleNotFoundError when migrating data
  [vangheem]


3.0.17 (2018-05-29)
-------------------

- Handle running migration when existing index does not exist
  [vangheem]

- Make sure to refresh object before writing to it.
  [vangheem]


3.0.16 (2018-05-29)
-------------------

- Raise QueryErrorException instead of returning it
  [vangheem]


3.0.15 (2018-05-25)
-------------------

- Fix update_by_query indexes param
  [vangheem]


3.0.14 (2018-05-25)
-------------------

- be able to provide context for update_by_query
  [vangheem]


3.0.13 (2018-05-25)
-------------------

- Fix vacuuming with sub indexes
  [vangheem]


3.0.12 (2018-05-24)
-------------------

- fix format_hit handling of list fields better
  [vangheem]


3.0.11 (2018-05-24)
-------------------

- Provide format_hit util
  [vangheem]


3.0.10 (2018-05-23)
-------------------

- Add more stored fields
  [vangheem]


3.0.9 (2018-05-23)
------------------

- add backoff for some elasticsearch operations
  [vangheem]


3.0.8 (2018-05-22)
------------------

- make IIndexManager.get_schemas async
  [vangheem]


3.0.7 (2018-05-21)
------------------

- Handle missing __parent__
  [vangheem]


3.0.6 (2018-05-21)
------------------

- Fix bug in reindexing security for children of sub indexes
  [vangheem]


3.0.5 (2018-05-21)
------------------

- Fix release
  [vangheem]


3.0.4 (2018-05-21)
------------------

- cleanup_es accepts a prefix value
  [vangheem]


3.0.3 (2018-05-21)
------------------

- provide get_index_for util
  [vangheem]

- provide cleanup_es test util
  [vangheem]

- fix storing annotation data on sub index
  [vangheem]

- get_by_path should accept index param
  [vangheem]


3.0.2 (2018-05-21)
------------------

- Fix release
  [vangheem]

- add utils.get_all_indexes_identifier
  [vangheem]


3.0.1 (2018-05-21)
------------------

- Create index with mappings/indexes instead of updating them after creation
  [vangheem]

- Add `es-fields` command to inspect configured fields
  [vangheem]


3.0.0 (2018-05-19)
------------------

- Add support for sub indexes
  [vangheem]

- Raise exception instead of returning ErrorResponse
  [vangheem]

- Add scroll support to query
  [gitcarbs]

2.0.1 (2018-05-10)
------------------

- Add more logging for migrations
  [vangheem]

- Added IIndexProgress to hook on reindex process
- Added new event on reindex start.
- Added context to the IIndexProgress event
  [jordic]


2.0.0 (2018-05-06)
------------------

- replace aioes(unsupported) with aioelasticsearch
  [vangheem]

- Elasticsearch 6 compatibility.
  [vangheem]


1.3.13 (2018-05-02)
-------------------

- Format stored field results like source results
  [vangheem]


1.3.12 (2018-05-01)
-------------------

- More vacuum improvements


1.3.11 (2018-04-30)
-------------------

- More vacuum improvements
  [vangheem]

1.3.10 (2018-04-30)
-------------------

- migrate script should not use transactions
  [vangheem]

1.3.9 (2018-04-30)
------------------

- Optimized vacuum script to use tid sorting which should prevent
  needing to go through so many docs to do the vacuum check
  [vangheem]


1.3.8 (2018-04-27)
------------------

- Provide `@name` in results
  [vangheem]


1.3.7 (2018-04-26)
------------------

- change `@id` in search results to `@uid`
  [vangheem]

- Add support for analysis character filter
  [gitcarbs]


1.3.6 (2018-04-18)
------------------

- Work with store=true mappings
  [vangheem]


1.3.5 (2018-04-15)
------------------

- Smaller bulk sizes for es vacuum
  [vangheem]


1.3.4 (2018-04-15)
------------------

- Some performance fixes for vacuuming
  [vangheem]


1.3.3 (2018-04-14)
------------------

- Provide profile data in results
  [gitcarbs]


1.3.2 (2018-04-03)
------------------

- Upgrade tests to use pytest-docker-fixtures
  [vangheem]


1.3.1 (2018-03-14)
------------------

- Pay attention to `index_data` configuration
  [vangheem]


1.3.0 (2018-03-14)
------------------

- Upgrade to work with guillotina 2.4.x
  [vangheem]


1.2.11 (2018-03-09)
-------------------

- Implement statement cache clearing
  [vangheem]


1.2.10 (2018-03-02)
-------------------

- Do not use cached statement for vacuum
  [vangheem]


1.2.9 (2018-02-07)
------------------

- Handle errors unpickling object for es vacuum
  [vangheem]


1.2.8 (2018-01-11)
------------------

- Make sure to change transaction strategy for commands
  [vangheem]


1.2.7 (2018-01-08)
------------------

- Make sure to close out connection in commands
  [vangheem]


1.2.6 (2017-12-18)
------------------

- Use `traverse` instead of `do_traverse`.
  [vangheem]


1.2.5 (2017-12-08)
------------------

- Retry on conflict for updates
  [vangheem]


1.2.4 (2017-12-06)
------------------

- Use futures instead of threads for migrations
  [vangheem]


1.2.3 (2017-11-21)
------------------

- Upgrade testing infrastructure with latest guillotina
  [vangheem]


1.2.2 (2017-11-08)
------------------

- Fix test setup with jenkins
  [vangheem]


1.2.1 (2017-11-08)
------------------

- Make logging less noisy
  [vangheem]


1.2.0 (2017-11-06)
------------------

- Upgrade to guillotina 2.0.0
  [vangheem]


1.1.24 (2017-10-12)
-------------------

- Close connection after done in vacuum command
  [vangheem]


1.1.23 (2017-10-09)
-------------------

- Make sure to use `async with` syntax for manual api calls to elasticsearch using
  session object.
  [vangheem]


1.1.22 (2017-09-28)
-------------------

- Another tweak for missing indexes on running migration
  [vangheem]


1.1.21 (2017-09-21)
-------------------

- All indexing/removing operations are already in a future so no need to add futures
  to operations.
  [vangheem]


1.1.20 (2017-09-21)
-------------------

- Use latest guillotina futures request api
  [vangheem]


1.1.19 (2017-09-14)
-------------------

- Change page size and scroll of esvacuum to prevent timeouts
  [vangheem]


1.1.18 (2017-08-22)
-------------------

- Fix TIDConflictError when registry is edited during a migration.
  [vangheem]


1.1.17 (2017-08-11)
-------------------

- Handle missing types from migrations when mappings change better
  [vangheem]


1.1.16 (2017-08-09)
-------------------

- Results from search should return sort value
  [gitcarbs]


1.1.15 (2017-07-28)
-------------------

- Fix vacuum to handle empty scroll errors when iterating through all keys
  [vangheem]


1.1.14 (2017-07-21)
-------------------

- Fix deadlock issue on unindex when migration is active
  [vangheem]


1.1.13 (2017-07-12)
-------------------

- Wait a bit before running migration so addons that use async to calculate
  dynamic types can load


1.1.12 (2017-07-12)
-------------------

- Fix scenario where doc type is missing in the upgraded mapping
  [vangheem]


1.1.11 (2017-07-10)
-------------------

- Add update_by_query method
  [vangheem]


1.1.10 (2017-07-06)
-------------------

- Add reindex command
  [vangheem]


1.1.9 (2017-07-06)
------------------

- Fix invalid import in migrate script
  [vangheem]


1.1.8 (2017-07-05)
------------------

- Add more logging for vacuum
  [vangheem]


1.1.7 (2017-06-29)
------------------

- Add vacuum command
  [vangheem]


1.1.6 (2017-06-23)
------------------

- Fix group query to not prepend 'group:' to security query for groups
  [vangheem]


1.1.5 (2017-06-21)
------------------

- Fix migration when objects were deleted while migrating that were thought to
  be orphaned
  [vangheem]


1.1.4 (2017-06-20)
------------------

- Prevent auto mapping of metadata
  [bloodbare]


1.1.3 (2017-06-15)
------------------

- Fix reindexing
  [vangheem]


1.1.2 (2017-06-15)
------------------

- Fix import for client_exceptions aiohttp errors
  [vangheem]


1.1.1 (2017-06-09)
------------------

- Move fixtures out of conftest.py into it's own module. This could break
  tests that depend on it!
  [vangheem]


1.1.0 (2017-06-09)
------------------

- Add Reindexer utility that specializes just in reindexing instead of
  using the migrator
  [vangheem]


1.0.29 (2017-06-08)
-------------------

- Clean mapping before it's compared to prevent false positives for detecting
  differences in mappings
  [vangheem]


1.0.28 (2017-06-08)
-------------------

- Be able to migrate the mapping only and skip working through content on the site
  [vangheem]


1.0.27 (2017-06-07)
-------------------

- Make sure to flush reindexed items when calling reindex_all_content
  [vangheem]


1.0.26 (2017-05-26)
-------------------

- When adding groups to query that is built, make sure to prepend each group with
  "group:" so we can distinguish groups from users and somehow can not potentially
  hack it where they add a "root" group they are a part of
  [vangheem]


1.0.25 (2017-05-26)
-------------------

- Handle potential issue where data is changing while you're doing the reindex
  [vangheem]


1.0.24 (2017-05-26)
-------------------

- Fix issue where a bad original index would screw up index diff calculation
  [vangheem]

- Fix migration failure issue before flipping new index on
  [vangheem]


1.0.23 (2017-05-25)
-------------------

- Fix reindexing on security change
  [vangheem]

1.0.22 (2017-05-19)
-------------------

- Be able to cancel migration and have it do some cleanup
  [vangheem]


1.0.21 (2017-05-19)
-------------------

- A bunch of migration fixes to make it more resilient
  [vangheem]


1.0.20 (2017-05-18)
-------------------

- Implement live migration command
  [vangheem]


1.0.19 (2017-05-16)
-------------------

- Reindex with clean option should delete, re-create index
  [vangheem]


1.0.18 (2017-05-16)
-------------------

- Use dummy cache on reindex for all request types
  [vangheem]

1.0.17 (2017-05-16)
-------------------

- Disable caching when reindexing
  [vangheem]


1.0.16 (2017-05-15)
-------------------

- Use threads when bulk indexing on elasticsearch to make sure to always
  keep elasticsearch busy.
  [vangheem]

- Forcing the update of mapping
  [bloodbare]


1.0.15 (2017-05-12)
-------------------

- close and open the indices to define the settings
  [bloodbare]

1.0.14 (2017-05-12)
-------------------

- Enabling size on query by type
  [bloodbare]


1.0.13 (2017-05-11)
-------------------

- Also set settings on force_mappings
  [bloodare]


1.0.12 (2017-05-11)
-------------------

- Changing permissions name to guillotina
  [bloodbare]


1.0.11 (2017-05-11)
-------------------

- Option to set the mapping without reindexing in case of mapping mutation
  [bloodbare]


1.0.10 (2017-05-09)
-------------------

- Fix --update-missing argument with `es-reindex` command
  [vangheem]


1.0.9 (2017-05-09)
------------------

- Fix bug when deleting nodes
  [bloodbare]


1.0.8 (2017-05-08)
------------------

- Improve performance of reindexing of bushy content by using asyncio.gather
  to traverse sub-trees and index at the same time.
  [vangheem]

- Improve options to reindex command
  [vangheem]


1.0.7 (2017-05-04)
------------------

- reindex_all_content takes update and update_missing params now
  [vangheem]


1.0.6 (2017-05-03)
------------------

- Be able to update from an existing reindex
  [vangheem]


1.0.5 (2017-05-02)
------------------

- Need to avoid using long running queries on reindex because they can timeout
  for very large folders
  [vangheem]


1.0.4 (2017-05-02)
------------------

- optimize reindex more
  [vangheem]


1.0.3 (2017-05-02)
------------------

- More memory efficient reindex
  [vangheem]


1.0.2 (2017-05-02)
------------------

- Fix reindexing content
  [vangheem]


1.0.1 (2017-04-25)
------------------

- Provide as async utility as it allows us to close connections when the object
  is destroyed
  [vangheem]


1.0.0 (2017-04-24)
------------------

- initial release
