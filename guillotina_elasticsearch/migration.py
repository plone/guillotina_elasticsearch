from guillotina import directives
from guillotina.catalog.catalog import DefaultCatalogDataAdapter
from guillotina.component import getUtilitiesFor
from guillotina.content import iter_schemata_for_type
from guillotina.db.cache.dummy import DummyCache
from guillotina.directives import merged_tagged_value_dict
from guillotina.exceptions import NoIndexField
from guillotina.interfaces import IAsyncBehavior
from guillotina.interfaces import ICatalogDataAdapter
from guillotina.interfaces import IContainer
from guillotina.interfaces import IFolder
from guillotina.interfaces import IInteraction
from guillotina.interfaces import IResourceFactory
from guillotina.interfaces import ISecurityInfo
from guillotina.transactions import managed_transaction
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina_elasticsearch.utility import ElasticSearchUtility
from guillotina_elasticsearch.utils import noop_response

import asyncio
import gc
import json
import logging
import resource
import threading
import time


logger = logging.getLogger('guillotina_elasticsearch')


class Indexer:

    def __init__(self):
        self.data_adapter = DefaultCatalogDataAdapter(None)
        self.mappings = {}
        for type_name, schema in getUtilitiesFor(IResourceFactory):
            self.mappings[type_name] = {}
            for schema in iter_schemata_for_type(type_name):
                for index_name, index_data in merged_tagged_value_dict(
                        schema, directives.index.key).items():
                    self.mappings[type_name][index_name] = {
                        'schema': schema,
                        'properties': index_data
                    }

    async def get_value(self, ob, index_name):
        try:
            schema = self.mappings[ob.type_name][index_name]['schema']
            index_data = self.mappings[ob.type_name][index_name]['properties']
        except KeyError:
            return None
        behavior = schema(ob)
        if IAsyncBehavior.implementedBy(behavior.__class__):
            # providedBy not working here?
            await behavior.load(create=False)
        try:
            if 'accessor' in index_data:
                return index_data['accessor'](behavior)
            else:
                return self.data_adapter.get_data(behavior, schema, index_name)
        except NoIndexField:
            pass


class ElasticThread(threading.Thread):
    def __init__(self, index_name, batch):
        self.index_name = index_name
        self.batch = batch
        super().__init__(target=self)

    def __call__(self):
        self._loop = asyncio.new_event_loop()
        self._loop.run_until_complete(self._run())

    async def _run(self):
        utility = ElasticSearchUtility(loop=self._loop)
        bulk_data = []

        uids = []
        for uuid, batch_type, data in self.batch:
            uids.append(uuid)
            doc_type = data['type_name']
            if batch_type == 'update':
                data = {'doc': data}
            bulk_data.append({
                batch_type: {
                    '_index': self.index_name,
                    '_type': doc_type,
                    '_id': uuid
                }
            })
            if batch_type != 'delete':
                bulk_data.append(data)
        results = await utility.conn.bulk(index=self.index_name, doc_type=None,
                                          body=bulk_data)
        if results['errors']:
            logger.warn(f'Error bulk bulk putting: {", ".join(uids)}')
        utility.conn.close()
        del utility
        del self.batch


def _clean_mapping(mapping):
    if 'properties' in mapping:
        for key in ('confirm',):
            if key in mapping['properties']:
                del mapping['properties'][key]
        if ('type' in mapping['properties'] and
                'fields' in mapping['properties']['type'] and
                isinstance(mapping['properties']['type']['fields'], dict)):
            del mapping['properties']['type']
    return mapping


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


    TODO:
        - optionally fill metadata in indexing
            - requires more work...
    '''

    def __init__(self, utility, context, response=noop_response, force=False,
                 log_details=False, memory_tracking=False, request=None,
                 bulk_size=40, full=False, reindex_security=False, mapping_only=False):
        self.utility = utility
        self.conn = utility.conn
        self.context = context
        self.response = response
        self.force = force
        self.full = full
        self.log_details = log_details
        self.memory_tracking = memory_tracking
        self.bulk_size = bulk_size
        self.reindex_security = reindex_security
        if mapping_only and full:
            raise Exception('Can not do a full reindex and a mapping only migration')
        self.mapping_only = mapping_only

        if request is None:
            self.request = get_current_request()
            self.request._db_write_enabled = False
        else:
            self.request = request
        # make sure that we don't cache requests...
        self.request._txn._cache = DummyCache(None, None)
        self.container = self.request.container
        self.interaction = IInteraction(self.request)
        self.indexer = Indexer()

        self.batch = []
        self.indexed = 0
        self.processed = 0
        self.missing = []
        self.orphaned = []
        self.existing = []
        self.errors = []
        self.mapping_diff = {}
        self.start_time = self.index_start_time = time.time()
        self.reindex_threads = []
        self.status = 'started'
        self.active_task_id = None

        self.copied_docs = 0

        self.work_index_name = None

    def per_sec(self):
        return self.processed / (time.time() - self.index_start_time)

    async def create_next_index(self):
        version = await self.utility.get_version(self.container,
                                                 request=self.request)
        next_version = version + 1
        index_name = await self.utility.get_index_name(self.container,
                                                       request=self.request)
        next_index_name = index_name + '_' + str(next_version)
        if await self.conn.indices.exists(next_index_name):
            if self.force:
                # delete and recreate
                self.response.write('Clearing index')
                resp = await self.conn.indices.delete(next_index_name)
                assert resp['acknowledged']
        await self.conn.indices.create(next_index_name)
        return next_version, next_index_name

    async def copy_to_next_index(self):
        conn_es = await self.conn.transport.get_connection()
        real_index_name = await self.utility.get_index_name(self.container,
                                                            self.request)
        resp = await conn_es._session.post(
            str(conn_es._base_url) + '_reindex',
            params={
                'wait_for_completion': 'false'
            },
            data=json.dumps({
              "source": {
                "index": real_index_name,
                "size": 100
              },
              "dest": {
                "index": self.work_index_name
              }
            })
        )

        data = await resp.json()
        self.active_task_id = task_id = data['task']
        while True:
            await asyncio.sleep(10)
            resp = await conn_es._session.get(
                str(conn_es._base_url) + '_tasks/' + task_id)
            if resp.status in (400, 404):
                break
            data = await resp.json()
            if data['completed']:
                break
            status = data["task"]["status"]
            self.response.write(f'{status["created"]}/{status["total"]} - '
                                f'Copying data to new index. task id: {task_id}')
            self.copied_docs = status["created"]

        self.active_task_id = None
        response = data['response']
        failures = response['failures']
        if len(failures) > 0:
            failures = json.dumps(failures, sort_keys=True, indent=4,
                                  separators=(',', ': '))
            self.response.write(f'Reindex encountered failures: {failures}')
        else:
            self.response.write(f'Finished copying to new index: {self.copied_docs}')

    async def get_all_uids(self):
        self.response.write('Retrieving existing doc ids')
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
                scroll='2m'
            )
            if len(result['hits']['hits']) == 0:
                break
            ids.extend([r['_id'] for r in result['hits']['hits']])
            self.response.write(f'Retrieved {len(ids)} doc ids')
            scroll_id = result['_scroll_id']
        self.response.write(f'Retrieved {len(ids)}. Copied {self.copied_docs} docs')
        return ids

    async def calculate_mapping_diff(self):
        '''
        all we care about is new fields...
        Missing ones are ignored and we don't care about it.
        '''
        diffs = {}
        existing_index_name = await self.utility.get_real_index_name(
            self.container, self.request)
        existing_mappings = await self.conn.indices.get_mapping(existing_index_name)
        existing_mappings = existing_mappings[existing_index_name]['mappings']
        next_mappings = await self.conn.indices.get_mapping(self.work_index_name)
        next_mappings = next_mappings[self.work_index_name]['mappings']

        for type_name, schema in getUtilitiesFor(IResourceFactory):
            new_definitions = {}
            if type_name not in existing_mappings:
                diffs[type_name] = next_mappings[type_name]['properties']
                continue

            existing_mapping = existing_mappings[type_name]['properties']
            next_mapping = next_mappings[type_name]['properties']

            for field_name, definition in next_mapping.items():
                definition = _clean_mapping(definition)
                if (field_name not in existing_mapping or
                        definition != _clean_mapping(existing_mapping[field_name])):
                    new_definitions[field_name] = definition
            if len(new_definitions) > 0:
                diffs[type_name] = new_definitions

        for type_name, mapping in existing_mappings.items():
            if type_name not in next_mappings:
                # special case here... we need to import this mapping still
                # in order for the index copy to work correctly if docs ref it
                self.response.write(f'Backporting mapping of {type_name} to new '
                                    f'even though it is not defined anymore')
                await self.conn.indices.put_mapping(
                    self.work_index_name, type_name, mapping)

        return diffs

    async def process_folder(self, ob):
        for key in await ob.async_keys():
            try:
                item = await ob._p_jar.get_child(ob, key)
            except KeyError:
                continue
            if item is None:
                continue
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
        self.processed += 1

        if IFolder.providedBy(ob):
            await self.process_folder(ob)

        if not IContainer.providedBy(ob):
            del ob.__annotations__
        del ob

    async def index_object(self, ob, full=False):
        batch_type = 'update'
        if self.reindex_security:
            data = ISecurityInfo(ob)()
        elif full or self.full:
            data = await ICatalogDataAdapter(ob)()
            batch_type = 'index'
        else:
            if ob.type_name not in self.mapping_diff:
                # no fields change, ignore this guy...
                if self.log_details:
                    self.response.write(f'({self.processed} {int(self.per_sec())}) '
                                        f'(skipped) Object: {get_content_path(ob)}, '
                                        f'Type: {batch_type}, Buffer: {len(self.batch)}')
                return
            data = {
                'type_name': ob.type_name  # always need this one...
            }
            for index_name in self.mapping_diff[ob.type_name].keys():
                val = await self.indexer.get_value(ob, index_name)
                if val is not None:
                    data[index_name] = val

        self.indexed += 1
        self.batch.append((ob.uuid, batch_type, data))

        if self.log_details:
            self.response.write(f'({self.processed} {int(self.per_sec())}) '
                                f'Object: {get_content_path(ob)}, '
                                f'Type: {batch_type}, Buffer: {len(self.batch)}')

        await self.attempt_flush()

    async def attempt_flush(self):

        if self.processed % 500 == 0:
            self.interaction.invalidate_cache()
            num, _, _ = gc.get_count()
            gc.collect()
            if self.memory_tracking:
                total_memory = round(
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024.0, 1)
                self.response.write(b'Memory usage: % 2.2f MB, cleaned: %d, total in-memory obs: %d' % (
                    total_memory, num, len(gc.get_objects())))
            self.response.write(b'Indexing new batch, totals: (%d %d/sec)\n' % (
                self.indexed, int(self.per_sec()),
            ))

        if len(self.batch) >= self.bulk_size:
            await self.flush()

    def join_threads(self):
        for thread in self.reindex_threads:
            thread.join()
        self.reindex_threads = []

    async def flush(self):
        thread = ElasticThread(
            self.work_index_name,
            self.batch
        )
        self.batch = []

        self.reindex_threads.append(thread)
        thread.start()

        if len(self.reindex_threads) > 7:
            self.join_threads()

    async def check_existing(self):
        '''
        Go through self.existing and see why it wasn't processed
        '''
        for uuid in self.existing:
            try:
                ob = await self.context._p_jar.get(uuid)
            except KeyError:
                ob = None
            if ob is None:
                # no longer present on db, this was orphaned
                self.orphaned.append(uuid)
                # this is dumb... since we don't have the doc type on us, we
                # need to ask elasticsearch for it again...
                # elasticsearch does not allow deleting without the doc type
                # even though you can query for a doc without it... argh
                doc = await self.conn.get(self.work_index_name, uuid,
                                          _source=False)
                self.batch.append((uuid, 'delete', {'type_name': doc['_type']}))
                await self.attempt_flush()
            else:
                # XXX this should not happen so log it. Maybe we'll try doing something
                # about it another time...
                self.errors.append({
                    'type': 'unprocessed',
                    'uuid': uuid
                })
                # we re-query es to get full path of ob
                # doc = self.utility.conn.get(
                #     await self.utility.get_index_name(), fields='path'
                # )
                # ob = await do_traverse(self.request, self.container,
                #                        doc['_source']['path'].strip('/').split('/'))
                # await self.index_object(ob, full=True)

    async def setup_next_index(self):
        self.response.write(b'Creating new index')
        async with managed_transaction(self.request, write=True, adopt_parent_txn=True):
            await self.utility.disable_next_index(self.context, request=self.request)

        async with managed_transaction(self.request, write=True, adopt_parent_txn=True):
            self.next_index_version, self.work_index_name = await self.create_next_index()
            await self.utility.install_mappings_on_index(self.work_index_name)
            await self.utility.activate_next_index(
                self.container, self.next_index_version, request=self.request,
                force=self.force)

    async def cancel_migration(self):
        # canceling the migration, clearing index
        self.response.write('Canceling migration')
        async with managed_transaction(self.request, write=True, adopt_parent_txn=True):
            await self.utility.disable_next_index(self.context, request=self.request)
            self.response.write('Next index disabled')
        if self.active_task_id is not None:
            self.response.write('Canceling copy of index task')
            conn_es = await self.conn.transport.get_connection()
            await conn_es._session.post(
                str(conn_es._base_url) + '_tasks/' + self.active_task_id + '/_cancel')
            asyncio.sleep(5)
        if self.work_index_name:
            self.response.write('Deleting new index')
            await self.conn.indices.delete(self.work_index_name)
        self.response.write('Migration canceled')

    async def run_migration(self):
        alias_index_name = await self.utility.get_index_name(self.container,
                                                             request=self.request)
        existing_index = await self.utility.get_real_index_name(self.container,
                                                                request=self.request)

        await self.setup_next_index()

        self.mapping_diff = await self.calculate_mapping_diff()
        diff = json.dumps(self.mapping_diff, sort_keys=True, indent=4,
                          separators=(',', ': '))
        self.response.write(f'Caculated mapping diff: {diff}')

        if not self.full:
            # if full, we're reindexing everything does not matter what anyways, so skip
            self.response.write('Copying initial index data from existing index into new')
            await self.copy_to_next_index()
            self.response.write('Copying initial index data finished')

        if not self.mapping_only:
            self.existing = await self.get_all_uids()

            self.index_start_time = time.time()
            await self.process_object(self.context)  # this is recursive

            await self.check_existing()

            await self.flush()
            self.join_threads()

        async with self.utility._migration_lock:
            self.response.write('Activating new index')
            async with managed_transaction(self.request, write=True, adopt_parent_txn=True):
                await self.utility.apply_next_index(self.container, self.request)
            self.status = 'done'

            self.response.write('Update aliases')
            await self.conn.indices.update_aliases({
                "actions": [
                    {"remove": {
                        "alias": alias_index_name,
                        "index": existing_index
                    }},
                    {"add": {
                        "alias": alias_index_name,
                        "index": self.work_index_name
                    }}
                ]
            })

        self.response.write('Delete old index')
        await self.conn.indices.close(existing_index)
        await self.conn.indices.delete(existing_index)
