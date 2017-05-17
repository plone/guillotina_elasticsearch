from guillotina.utils import get_current_request
from guillotina.db.cache.dummy import DummyCache
import time
from guillotina.interfaces import IInteraction, IFolder
from guillotina.transactions import managed_transaction


class Migrator:
    '''
    Reindex/Migration...
    Reindex is going to behave much the same as migration would so we're using
    this for both cases most of the time...

    In order to do a *live* reindex, we need to follow these steps...

    1. Create a next index
        - if already exists, fail
            - unless, "force" option provided
    2. Put new mapping on next index
    3. Mark that there is a new next index on the container object
    4. All new index/delete operations are done on...
        - new index
        - and existing index
        - ... existing queries stay on current index
    5. Copy existing index data over to the next index
    6. Get a list of all existing doc ids on index
    7. Take diff of existing mapping to new mapping
    8. Crawl content
        - check if doc does not exist
            - make sure it wasn't added in the mean time
                - if it was, do an update with diff instead
            - record it
            - do complete index
        - if doc exist
            - if diff mapping exists
                - update fields in diff on doc
            - else, do nothing
            - remove for list of existing doc ids
    9. Go through list of existing doc ids
        - double check not on container(query db)
        - delete doc if not in container
            - record it
    10. Refresh db container ob
    11. Point alias at next index
    12. Delete old index
    '''

    def __init__(self, utility, context, response=None, force=False,
                 log_details=False, memory_tracking=False, request=None):
        self.utility = utility
        self.conn = utility.conn
        self.context = context
        self.response = response
        self.force = force
        self.log_details = log_details
        self.memory_tracking = memory_tracking

        if request is None:
            self.request = get_current_request()
            self.request._db_write_enabled = False
        else:
            self.request = request
        # make sure that we don't cache requests...
        self.request._txn._cache = DummyCache(None, None)
        self.container = self.request.container
        self.interaction = IInteraction(self.request)

        self.batch = {}
        self.indexed = 0
        self.missing = []
        self.orphaned = []
        self.existing = []
        self.mapping_diff = {}
        self.start_time = time.time()
        self.reindex_threads = []

        self.next_index_name = None

    async def create_next_index(self):
        version = await self.get_version(self.container)
        next_version = version + 1
        index_name = await self.get_index_name(self.container, self.request)
        next_index_name = index_name + '_' + str(next_version)
        if self.conn.indices.exists(next_index_name):
            if self.force:
                # delete and recreate
                await self.conn.indices.delete(next_index_name)
        await self.conn.indices.create(next_index_name)
        return next_version, next_index_name

    async def copy_to_next_index(self):
        pass

    async def get_all_uids(self):
        page_size = 3000
        ids = []
        index_name = await self.utility.get_index_name(self.container)
        result = await self.conn.search(
            index=index_name,
            scroll='2m',
            size=page_size,
            stored_fields='',
            body={
                "sort": ["_doc"]
            })
        ids.extend([r['_id'] for r in result['hits']['hits']])
        scroll_id = result['_scroll_id']
        while scroll_id:
            result = await self.utility.conn.scroll(
                scroll_id=scroll_id,
                scroll='30s'
            )
            if len(result['hits']['hits']) == 0:
                break
            ids.extend([r['_id'] for r in result['hits']['hits']])
            scroll_id = result['_scroll_id']
        return ids

    async def calculate_mapping_diff(self):
        pass

    async def process_folder(self, ob):
        for key in await ob.async_keys():
            # if key in already_visited:
            #     continue
            item = await ob._p_jar.get_child(ob, key)
            await self.process_object(item)
            del item

        del ob

    async def process_object(self, ob):
        '''
        - check if doc does not exist
            - record it
            - do complete index
        - if doc exist
            - if diff mapping exists
                - update fields in diff on doc
            - else, do nothing
            - remove for list of existing doc ids
        '''
        # do stuff...
        full = False
        if ob.uuid not in self.existing:
            self.missing.append(ob.uuid)
            full = True
        else:
            self.existing.remove(ob.uuid)
        await self.index_object(ob, full=full)

        if IFolder.providedBy(self.context):
            await self.process_folder(ob)

    async def index_object(self, ob, full=False):
        pass

    async def check_missing(self):
        '''
        Go through self.existing and see why it wasn't processed
        '''
        for uuid in self.existing:
            ob = self.context._txn.get(uuid)
            if ob is None:
                # no longer present on db, this was orphaned
                self.orphaned.append(uuid)
                # XXX delete doc
            else:
                # XXX fill in parents and do a full index
                pass

    async def run_migration(self):
        alias_index_name = await self.utility.get_index_name(self.container)
        existing_index = await self.utility.get_real_index_name(self.container)

        async with managed_transaction(self.request, allow_write=True):
            self.next_index_version, self.next_index_name = await self.create_next_index()
            await self.utility.install_mappings_on_index(self.next_index_name)
            await self.utility.activate_next_index(self.container, self.next_index_version)

        await self.copy_to_next_index()

        self.existing = await self.get_all_uids()

        self.mapping_diff = await self.calculate_mapping_diff()

        await self.process_object(self.context)

        await self.check_missing()

        async with managed_transaction(self.request, allow_write=True):
            await self.utility.apply_next_index(self.container)

        await self.conn.indices.update_aliases({
            "actions": [
                {"remove": {
                    "alias": alias_index_name,
                    "index": existing_index
                }},
                {"add": {
                    "alias": alias_index_name,
                    "index": self.next_index_name
                }}
            ]
        })
        await self.conn.indices.close(existing_index)
        await self.conn.indices.delete(existing_index)
