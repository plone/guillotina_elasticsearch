from elasticsearch import AsyncElasticsearch
from guillotina import directives
from guillotina.catalog.catalog import DefaultCatalogDataAdapter
from guillotina.component import get_adapter
from guillotina.component import get_utilities_for
from guillotina.content import iter_schemata_for_type
from guillotina.db.cache.dummy import DummyCache
from guillotina.directives import merged_tagged_value_dict
from guillotina.event import notify
from guillotina.exceptions import NoIndexField
from guillotina.interfaces import IAsyncBehavior
from guillotina.interfaces import ICatalogDataAdapter
from guillotina.interfaces import IContainer
from guillotina.interfaces import IFolder
from guillotina.interfaces import IResourceFactory
from guillotina.interfaces import ISecurityInfo
from guillotina.transactions import transaction
from guillotina.utils import apply_coroutine
from guillotina.utils import get_authenticated_user
from guillotina.utils import get_content_path
from guillotina.utils import get_current_container
from guillotina.utils import get_current_transaction
from guillotina.utils import get_security_policy
from guillotina_elasticsearch.events import IndexProgress
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.utils import get_migration_lock
from guillotina_elasticsearch.utils import noop_response

import asyncio
import backoff
import elasticsearch.exceptions
import gc
import json
import logging
import resource
import time


logger = logging.getLogger("guillotina_elasticsearch")


class Indexer:
    def __init__(self):
        self.data_adapter = DefaultCatalogDataAdapter(None)
        self.mappings = {}
        for type_name, schema in get_utilities_for(IResourceFactory):
            self.mappings[type_name] = {}
            for schema in iter_schemata_for_type(type_name):
                for field_name, index_data in merged_tagged_value_dict(
                    schema, directives.index.key
                ).items():
                    index_name = index_data.get("index_name", field_name)
                    self.mappings[type_name][index_name] = {
                        "schema": schema,
                        "properties": index_data,
                    }

    async def get_value(self, ob, index_name):
        try:
            schema = self.mappings[ob.type_name][index_name]["schema"]
            index_data = self.mappings[ob.type_name][index_name]["properties"]
        except KeyError:
            return None
        behavior = schema(ob)
        if IAsyncBehavior.implementedBy(behavior.__class__):
            # providedBy not working here?
            await behavior.load(create=False)
        try:
            if "accessor" in index_data:
                return await apply_coroutine(index_data["accessor"], behavior)
            else:
                return self.data_adapter.get_data(behavior, schema, index_name)
        except NoIndexField:
            pass


def _clean_mapping(mapping):
    if "properties" in mapping:
        for key in ("confirm",):
            if key in mapping["properties"]:
                del mapping["properties"][key]
        if (
            "type" in mapping["properties"]
            and "fields" in mapping["properties"]["type"]
            and isinstance(mapping["properties"]["type"]["fields"], dict)
        ):
            del mapping["properties"]["type"]
    return mapping


class Migrator:
    """
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
    """

    def __init__(
        self,
        utility,
        context,
        response=noop_response,
        force=False,
        log_details=False,
        memory_tracking=False,
        request=None,
        bulk_size=40,
        full=False,
        reindex_security=False,
        mapping_only=False,
        index_manager=None,
        children_only=False,
        cache=True,
    ):
        self.utility = utility
        self.context = context
        self.response = response
        self.force = force
        self.full = full
        self.log_details = log_details
        self.memory_tracking = memory_tracking
        self.bulk_size = bulk_size
        self.reindex_security = reindex_security
        self.children_only = children_only
        if mapping_only and full:
            raise Exception("Can not do a full reindex and a mapping only migration")
        self.mapping_only = mapping_only

        self.txn = get_current_transaction()
        if not cache:
            # make sure that we don't cache requests...
            self.txn._cache = DummyCache(self.txn)

        self.request = request
        self.container = get_current_container()
        self.conn: AsyncElasticsearch = utility.get_connection()

        if index_manager is None:
            self.index_manager = get_adapter(self.container, IIndexManager)
        else:
            self.index_manager = index_manager

        self.user = get_authenticated_user()
        self.policy = get_security_policy(self.user)
        self.indexer = Indexer()

        self.batch = {}
        self.indexed = 0
        self.processed = 0
        self.missing = []
        self.orphaned = []
        self.existing = []
        self.errors = []
        self.mapping_diff = {}
        self.start_time = self.index_start_time = time.time()
        self.reindex_futures = []
        self.status = "started"
        self.active_task_id = None

        self.copied_docs = 0

        self.work_index_name = None

    def per_sec(self):
        return self.processed / (time.time() - self.index_start_time)

    async def create_next_index(self):
        async with transaction(adopt_parent_txn=True) as txn:
            await txn.refresh(await self.index_manager.get_registry())
            next_index_name = await self.index_manager.start_migration()
            # The index is created in the same transaction the registry is updated
            # to prevent another process from accessing the 'next_index' and not finding it
            if await self.conn.indices.exists(next_index_name):
                if self.force:
                    # delete and recreate
                    self.response.write("Clearing index")
                    resp = await self.conn.indices.delete(next_index_name)
                    assert resp["acknowledged"]
            await self.utility.create_index(next_index_name, self.index_manager)
        return next_index_name

    async def copy_to_next_index(self):
        real_index_name = await self.index_manager.get_index_name()
        data = await self.conn.reindex(
            {
                "source": {"index": real_index_name, "size": 100},
                "dest": {"index": self.work_index_name},
            },
            params={"wait_for_completion": "false"},
        )
        self.active_task_id = task_id = data["task"]
        task_completed = False
        while not task_completed:
            await asyncio.sleep(10)
            data = await self.conn.tasks.get(task_id)
            task_completed = data["completed"]
            if task_completed:
                break
            status = data["task"]["status"]
            self.response.write(
                f'{status["created"]}/{status["total"]} - '
                f"Copying data to new index. task id: {task_id}"
            )
            self.copied_docs = status["created"]

        self.active_task_id = None
        if task_completed:
            response = data["response"]
            failures = response["failures"]
            if len(failures) > 0:
                failures = json.dumps(
                    failures, sort_keys=True, indent=4, separators=(",", ": ")
                )
                self.response.write(f"Reindex encountered failures: {failures}")
            else:
                self.response.write(
                    f"Finished copying to new index: {self.copied_docs}"
                )
        else:
            self.response.write(f"Unknown state for task {task_id}")

    async def get_all_uids(self):
        self.response.write("Retrieving existing doc ids")
        page_size = 3000
        ids = []
        index_name = await self.index_manager.get_index_name()
        result = await self.conn.search(
            index=index_name,
            scroll="2m",
            size=page_size,
            stored_fields="",
            _source=False,
            body={"sort": ["_doc"]},
        )
        ids.extend([r["_id"] for r in result["hits"]["hits"]])
        scroll_id = result["_scroll_id"]
        while scroll_id:
            result = await self.conn.scroll(scroll_id=scroll_id, scroll="2m")
            if len(result["hits"]["hits"]) == 0:
                break
            ids.extend([r["_id"] for r in result["hits"]["hits"]])
            self.response.write(f"Retrieved {len(ids)} doc ids")
            scroll_id = result["_scroll_id"]
        self.response.write(f"Retrieved {len(ids)}. Copied {self.copied_docs} docs")
        return ids

    async def calculate_mapping_diff(self):
        """
        all we care about is new fields...
        Missing ones are ignored and we don't care about it.
        """
        next_mappings = await self.conn.indices.get_mapping(self.work_index_name)
        next_mappings = next_mappings[self.work_index_name]["mappings"]["properties"]

        existing_index_name = await self.index_manager.get_real_index_name()
        try:
            existing_mappings = await self.conn.indices.get_mapping(existing_index_name)
        except elasticsearch.exceptions.NotFoundError:
            # allows us to upgrade when no index is present yet
            return next_mappings

        existing_mappings = existing_mappings[existing_index_name]["mappings"][
            "properties"
        ]

        new_definitions = {}
        for field_name, definition in next_mappings.items():
            definition = _clean_mapping(definition)
            if field_name not in existing_mappings or definition != _clean_mapping(
                existing_mappings[field_name]
            ):  # noqa
                new_definitions[field_name] = definition
        return new_definitions

    async def process_folder(self, ob):
        txn = get_current_transaction()
        for key in await ob.async_keys():
            try:
                item = await txn.get_child(ob, key)
            except (KeyError, ModuleNotFoundError):
                continue
            if item is None:
                continue
            await self.process_object(item)
            del item

        del ob

    async def process_object(self, ob):
        """
        - check if doc does not exist
            - record it
            - do complete index
        - if doc exist
            - if diff mapping exists
                - update fields in diff on doc
            - else, do nothing
            - remove for list of existing doc ids
        """
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
            try:
                del self.container.__gannotations__
            except AttributeError:
                del self.container.__annotations__
        del ob

    async def index_object(self, ob, full=False):
        batch_type = "update"
        if full or self.full:
            try:
                data = await ICatalogDataAdapter(ob)()
            except TypeError:
                self.response.write(f"Could not index {ob}")
                return
            batch_type = "index"
        elif self.reindex_security:
            try:
                data = await apply_coroutine(ISecurityInfo(ob))
            except TypeError:
                self.response.write(f"Could not index {ob}")
                return
        else:
            data = {
                # always need these...
                "type_name": ob.type_name
            }
            for index_name in self.mapping_diff.keys():
                val = await self.indexer.get_value(ob, index_name)
                if val is not None:
                    data[index_name] = val

        if ob.__serial__:
            data["tid"] = ob.__serial__
        self.indexed += 1
        self.batch[ob.uuid] = {"action": batch_type, "data": data}

        if self.log_details:
            self.response.write(
                f"({self.processed} {int(self.per_sec())}) "
                f"Object: {get_content_path(ob)}, "
                f"Type: {batch_type}, Buffer: {len(self.batch)}"
            )

        await self.attempt_flush()

    async def attempt_flush(self):

        if self.processed % 500 == 0:
            self.policy.invalidate_cache()
            num, _, _ = gc.get_count()
            gc.collect()
            if self.memory_tracking:
                total_memory = round(
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0, 1
                )  # noqa
                self.response.write(
                    b"Memory usage: % 2.2f MB, cleaned: %d, total in-memory obs: %d"
                    % (total_memory, num, len(gc.get_objects()))  # noqa
                )
            self.response.write(
                b"Indexing new batch, totals: (%d %d/sec)\n"
                % (self.indexed, int(self.per_sec()))  # noqa
            )
        if len(self.batch) >= self.bulk_size:
            await notify(
                IndexProgress(
                    self.context,
                    self.processed,
                    (len(self.existing) + len(self.missing)),
                    request=self.request,
                )
            )
            await self.flush()

    async def join_futures(self):
        for future in self.reindex_futures:
            if not future.done():
                await asyncio.wait_for(future, None)
        self.reindex_futures = []

    @backoff.on_exception(
        backoff.constant,
        (asyncio.TimeoutError, elasticsearch.exceptions.ConnectionTimeout),
        interval=1,
        max_tries=5,
    )
    async def _index_batch(self, batch):
        bulk_data = []
        for _id, payload in batch.items():
            index = payload.pop("__index__", self.work_index_name)
            action_data = {"_index": index, "_id": _id}
            data = payload["data"]
            if payload["action"] == "update":
                data = {"doc": data}
                action_data["retry_on_conflict"] = 3
            bulk_data.append({payload["action"]: action_data})
            if payload["action"] != "delete":
                bulk_data.append(data)
        results = await self.conn.bulk(index=self.work_index_name, body=bulk_data)
        if results["errors"]:
            errors = []
            for result in results["items"]:
                for key, value in result.items():
                    if not isinstance(value, dict):
                        continue
                    if "status" in value and value["status"] not in (200, 201):
                        _id = value.get("_id")

                        # retry conflict errors and thread pool exceeded errors
                        if value["status"] in (409, 429):
                            self.batch[_id] = batch[_id]
                        elif value["status"] == 404:
                            self.batch[_id] = batch[_id]
                            self.batch[_id]["action"] = "index"
                        else:
                            errors.append(f'{_id}: {value["status"]}')
            if len(errors) > 0:
                logger.warning(f"Error bulk putting: {errors}")

    async def flush(self):
        if len(self.batch) == 0:
            # nothing to flush
            return

        future = asyncio.ensure_future(self._index_batch(self.batch))
        self.batch = {}
        self.reindex_futures.append(future)

        if len(self.reindex_futures) > 7:
            await self.join_futures()

    async def check_existing(self):
        """
        Go through self.existing and see why it wasn't processed
        """
        for uuid in self.existing:
            try:
                ob = await self.context.__txn__.get(uuid)
            except KeyError:
                ob = None
            if ob is None:
                try:
                    self.batch[uuid] = {"action": "delete", "data": {}}
                    await self.attempt_flush()
                    # no longer present on db, this was orphaned
                    self.orphaned.append(uuid)
                except elasticsearch.exceptions.NotFoundError:
                    # it was deleted in the meantime so we're actually okay
                    self.orphaned.append(uuid)
            else:
                # XXX this should not happen so log it. Maybe we'll try
                # doing something about it another time...
                self.errors.append({"type": "unprocessed", "uuid": uuid})

    async def setup_next_index(self):
        self.response.write(b"Creating new index")
        async with get_migration_lock(await self.index_manager.get_index_name()):
            self.work_index_name = await self.create_next_index()
            return self.work_index_name

    async def cancel_migration(self):
        # canceling the migration, clearing index
        self.response.write("Canceling migration")
        async with transaction(adopt_parent_txn=True):
            await self.index_manager.cancel_migration()
            self.response.write("Next index disabled")
        if self.active_task_id is not None:
            self.response.write("Canceling copy of index task")
            await self.conn.tasks.cancel(self.active_task_id)
            await asyncio.sleep(5)
        if self.work_index_name:
            self.response.write("Deleting new index")
            await self.conn.indices.delete(self.work_index_name)
        self.response.write("Migration canceled")

    async def run_migration(self):
        alias_index_name = await self.index_manager.get_index_name()
        existing_index = await self.index_manager.get_real_index_name()

        await self.setup_next_index()

        self.mapping_diff = await self.calculate_mapping_diff()
        diff = json.dumps(
            self.mapping_diff, sort_keys=True, indent=4, separators=(",", ": ")
        )
        self.response.write(f"Caculated mapping diff: {diff}")

        if not self.full:
            # if full, we're reindexing everything does not matter what
            # anyways, so skip
            self.response.write(
                f"Copying initial index {existing_index} "
                f"into {self.work_index_name}"
            )
            try:
                await self.copy_to_next_index()
                self.response.write("Copying initial index data finished")
            except elasticsearch.exceptions.NotFoundError:
                self.response.write("No initial index to copy to")
        if not self.mapping_only:
            try:
                self.existing = await self.get_all_uids()
            except elasticsearch.exceptions.NotFoundError:
                pass

            self.index_start_time = time.time()
            if self.children_only or IContainer.providedBy(self.context):
                await self.process_folder(self.context)  # this is recursive
            else:
                await self.process_object(self.context)  # this is recursive

            await self.check_existing()

            await self.flush()
            await self.join_futures()

        async with get_migration_lock(await self.index_manager.get_index_name()):
            self.response.write("Activating new index")
            async with transaction(adopt_parent_txn=True):
                await self.index_manager.finish_migration()
            self.status = "done"

            self.response.write(
                f"""Update alias({alias_index_name}):
{existing_index} -> {self.work_index_name}
"""
            )

            try:
                await self.conn.indices.update_aliases(
                    {
                        "actions": [
                            {
                                "remove": {
                                    "alias": alias_index_name,
                                    "index": existing_index,
                                }
                            },
                            {
                                "add": {
                                    "alias": alias_index_name,
                                    "index": self.work_index_name,
                                }
                            },
                        ]
                    }
                )
            except elasticsearch.exceptions.NotFoundError:
                await self.conn.indices.update_aliases(
                    {
                        "actions": [
                            {
                                "add": {
                                    "alias": alias_index_name,
                                    "index": self.work_index_name,
                                }
                            }
                        ]
                    }
                )

        try:
            await self.conn.indices.close(existing_index)
            await self.conn.indices.delete(existing_index)
            self.response.write("Old index deleted")
        except elasticsearch.exceptions.NotFoundError:
            pass
