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
