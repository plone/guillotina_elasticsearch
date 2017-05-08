from guillotina.interfaces import ICatalogDataAdapter
from guillotina.interfaces import IFolder
from guillotina.interfaces import ISecurityInfo
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request

import asyncio
import gc
import resource
import time


class Counter:

    def __init__(self):
        self.indexed = 0
        self.start_time = time.time()

    def per_sec(self):
        return self.indexed / (time.time() - self.start_time)


class Reindexer:
    _sub_item_batch_size = 5

    def __init__(self, utility, context, security=False, response=None,
                 clean=True, update=False, update_missing=False,
                 log_details=False, memory_tracking=False,
                 request=None, counter=None):
        self.utility = utility
        self.context = context
        self.security = security
        self.response = response
        self.clean = clean
        self.update = update
        self.update_missing = update_missing

        if request is None:
            self.request = get_current_request()
            self.request._db_write_enabled = False
        else:
            self.request = request
        self.container = self.request.container
        self.batch = {}
        self.log_details = log_details
        self.memory_tracking = memory_tracking
        self.base_reindexer = None
        self.lock = asyncio.Lock()

        if counter is None:
            self.counter = Counter()
        else:
            self.counter = counter

    def clone(self, context):
        reindexer = Reindexer(
            self.utility, context,
            security=self.security, response=self.response,
            clean=self.clean, update=self.update, update_missing=self.update_missing,
            log_details=self.log_details, memory_tracking=self.memory_tracking,
            request=self.request, counter=self.counter)
        if self.base_reindexer is None:
            # first time splitting, this is the base_reindexer
            reindexer.base_reindexer = self
        else:
            reindexer.base_reindexer = self.base_reindexer
        return reindexer

    async def all_content(self):
        if not self.utility.enabled:
            return
        if (self.security is False and self.clean is True and self.update is False
                and self.update_missing is False):
            await self.utility.unindex_all_childs(self.context, response=self.response,
                                                  future=False)

        skip = []
        if self.update_missing:
            skip = await self.get_all_uids()

        if self.context.uuid not in skip:
            await self.add_object(obj=self.context)

        await self.index_sub_elements(obj=self.context)

        if len(self.batch) > 0:
            await self.reindex_bulk()

    async def get_all_uids(self):
        page_size = 700
        ids = []
        index_name = await self.utility.get_index_name(self.container)
        result = await self.conn.search(
            index=index_name,
            scroll='30s',
            size=page_size,
            stored_fields='',
            body={
                "query": {
                    "match_all": {}
                }
            })
        ids.extend([r['_id'] for r in result['hits']['hits']])
        scroll_id = result['_scroll_id']
        while scroll_id:
            result = await self.conn.scroll(
                scroll_id=scroll_id,
                scroll='30s'
            )
            if len(result['hits']['hits']) == 0:
                break
            ids.extend([r['_id'] for r in result['hits']['hits']])
            scroll_id = result['_scroll_id']
        return ids

    async def index_sub_elements(self, obj, skip=[], finish_batch=False):

        local_count = 0
        # we need to get all the keys because using async_items can cause the cursor
        # to be open for a long time on large containers. So long in fact, that
        # it'll timeout and bork the whole thing
        keys = await obj.async_keys()
        batch = []
        for key in keys:
            item = await obj._p_jar.get_child(obj, key)  # avoid event triggering
            if item.uuid not in skip:
                await self.add_object(obj=item)
            local_count += 1
            if IFolder.providedBy(item):
                reindexer = self.clone(item)
                # if we're going to use gather, each needs it's own batch
                batch.append(
                    reindexer.index_sub_elements(item, skip=skip, finish_batch=True))

            if len(batch) >= self._sub_item_batch_size:
                await asyncio.gather(*batch)
                batch = []
            del item
        if len(batch) > 0:
            await asyncio.gather(*batch)
            batch = []

        del obj

        if self.base_reindexer is not None:
            # propagate rest up to self.base_reindexer
            async with self.base_reindexer.lock:
                self.base_reindexer.batch.update(self.batch)
            await self.base_reindexer.attempt_flush()

    async def add_object(self, obj):
        if not self.utility.enabled:
            return
        self.counter.indexed += 1
        serialization = None
        if self.log_details and self.response is not None and hasattr(obj, 'id'):
            self.response.write(
                b'(%d %d/sec)Object: %s, Security: %r, Buffer: %d\n' % (
                    self.counter.indexed, int(self.counter.per_sec()),
                    get_content_path(obj).encode('utf-8'), self.security,
                    len(self.batch)))
        try:
            if self.security:
                serialization = ISecurityInfo(obj)()
            else:
                serialization = await ICatalogDataAdapter(obj)()
            self.batch[obj.uuid] = serialization
        except TypeError:
            pass

        await self.attempt_flush()

    async def attempt_flush(self):

        if self.memory_tracking and self.counter.indexed % 500 == 0:
            num, _, _ = gc.get_count()
            gc.collect()
            if self.response is not None:
                total_memory = round(
                    resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024.0/1024.0, 1)
                self.response.write(b'Memory usage: % 2.2f MB, cleaned: %d, total obs: %d' % (
                    total_memory, num, len(gc.get_objects())))

        async with self.lock:
            # we're working on batch, we need to lock it.
            if len(self.batch) >= self.utility.bulk_size:
                if self.response is not None:
                    self.response.write(b'Indexing new batch, totals: (%d %d/sec)\n' % (
                        self.counter.indexed, int(self.counter.per_sec()),
                    ))
                await self.reindex_bulk()
                self.batch.clear()

    async def reindex_bulk(self):
        if self.security or self.update:
            await self.utility.update(self.container, self.batch,
                                      response=self.response, flush_all=True)
        else:
            await self.utility.index(self.container, self.batch,
                                     response=self.response, flush_all=True)
