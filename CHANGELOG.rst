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
