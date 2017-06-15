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
