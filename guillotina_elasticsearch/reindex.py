from guillotina.interfaces import ICatalogDataAdapter
from guillotina.interfaces import IContainer
from guillotina.interfaces import IFolder
from guillotina.interfaces import IInteraction
from guillotina.interfaces import ISecurityInfo
from guillotina.utils import get_content_path
from guillotina.utils import get_current_request
from guillotina_elasticsearch.utility import ElasticSearchUtility

import asyncio
import gc
import resource
import threading
import time


class Counter:

    def __init__(self):
        self.indexed = 0
        self.start_time = time.time()

    def per_sec(self):
        return self.indexed / (time.time() - self.start_time)


BULK_SIZE = 200


class ReindexElasticSearchUtility(ElasticSearchUtility):

    bulk_size = BULK_SIZE

    def __init__(self, index_name, version, settings, loop):
        self._index_name = index_name
        self._version = version
        super().__init__(settings, loop)

    async def get_index_name(self, container):
        return self._index_name

    async def get_version(self, container):
        return self._version


class ElasticThread(threading.Thread):
    def __init__(self, index_name, version, batch, update=False, response=None):
        self.index_name = index_name
        self.version = version
        self.batch = batch
        self.update = update
        self.response = response
        super().__init__(target=self)

    def __call__(self):
        self._loop = asyncio.new_event_loop()
        self._loop.run_until_complete(self._run())

    async def _run(self):
        utility = ReindexElasticSearchUtility(self.index_name, self.version, {}, self._loop)
        if self.update:
            await utility.update(None, self.batch,
                                 response=self.response, flush_all=True)
        else:
            await utility.index(None, self.batch,
                                response=self.response, flush_all=True)
        utility._conn.close()


class Reindexer:
    _sub_item_batch_size = 5
    _connection_size = 20

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

        self.reindex_threads = []
        self.interaction = IInteraction(self.request)

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

        if IFolder.providedBy(self.context):
            await self.index_sub_elements(obj=self.context)

        if len(self.batch) > 0:
            await self.reindex_bulk()

        for thread in self.reindex_threads:
            thread.join()
        self.reindex_threads = []

    async def get_all_uids(self):
        page_size = 700
        ids = []
        index_name = await self.utility.get_index_name(self.container)
        result = await self.utility.conn.search(
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
            result = await self.utility.conn.scroll(
                scroll_id=scroll_id,
                scroll='30s'
            )
            if len(result['hits']['hits']) == 0:
                break
            ids.extend([r['_id'] for r in result['hits']['hits']])
            scroll_id = result['_scroll_id']
        return ids

    async def index_sub_elements(self, obj, skip=[]):

        # we need to get all the keys because using async_items can cause the cursor
        # to be open for a long time on large containers. So long in fact, that
        # it'll timeout and bork the whole thing
        keys = await obj.async_keys()
        if len(keys) > 500 and self.response is not None:
            self.response.write(b'Indexing large folder(%d) %s\n' % (
                len(keys),
                get_content_path(obj).encode('utf8')
            ))
        for key in keys:
            item = await obj._p_jar.get_child(obj, key)  # avoid event triggering
            if item.uuid not in skip:
                await self.add_object(obj=item)
            if IFolder.providedBy(item):
                await self.index_sub_elements(item, skip=skip)
            del item

        del obj

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
            if not IContainer.providedBy(obj):
                del obj.__annotations__
        except TypeError:
            pass

        await self.attempt_flush()

    async def attempt_flush(self):
        if self.counter.indexed > 2400:
            self.log_details = True

        if self.counter.indexed % 500 == 0:
            self.interaction.invalidate_cache()
            num, _, _ = gc.get_count()
            gc.collect()
            # import objgraph
            # objgraph.show_growth(limit=10)
            if self.response is not None:
                if self.memory_tracking:
                    total_memory = round(
                        resource.getrusage(resource.RUSAGE_SELF).ru_maxrss/1024.0, 1)
                    self.response.write(b'Memory usage: % 2.2f MB, cleaned: %d, total in-memory obs: %d' % (
                        total_memory, num, len(gc.get_objects())))
                self.response.write(b'Indexing new batch, totals: (%d %d/sec)\n' % (
                    self.counter.indexed, int(self.counter.per_sec()),
                ))

        if len(self.batch) >= BULK_SIZE:
            await self.reindex_bulk()
            self.batch.clear()

    async def get_index_name(self):
        if not hasattr(self, '_index_name'):
            self._index_name = await self.utility.get_index_name(self.container)
        return self._index_name

    async def get_version(self):
        if not hasattr(self, '_version'):
            self._version = await self.utility.get_version(self.container)
        return self._version

    async def reindex_bulk(self):
        thread = ElasticThread(
            await self.get_index_name(),
            await self.get_version(),
            self.batch.copy(), self.security or self.update,
            response=self.response
        )
        self.reindex_threads.append(thread)
        thread.start()

        if len(self.reindex_threads) > 7:
            if self.response is not None:
                self.response.write(b'Flushing reindex threads\n')
            for thread in self.reindex_threads:
                thread.join()
            self.reindex_threads = []
