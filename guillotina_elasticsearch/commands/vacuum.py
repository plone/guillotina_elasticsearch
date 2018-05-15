from guillotina.commands import Command
from guillotina.commands.utils import change_transaction_strategy
from guillotina.component import get_adapter
from guillotina.component import get_utility
from guillotina.db import ROOT_ID
from guillotina.db import TRASHED_ID
from guillotina.db.reader import reader
from guillotina.interfaces import ICatalogUtility
from guillotina.utils import get_containers
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.migration import Migrator
from lru import LRU  # pylint: disable=E0611
from os.path import join
from guillotina_elasticsearch.utils import get_content_sub_indexes
from guillotina_elasticsearch.utils import get_installed_sub_indexes

import aioelasticsearch
import asyncio
import json
import logging


try:
    from guillotina.utils import clear_conn_statement_cache
except ImportError:
    def clear_conn_statement_cache(conn):
        pass

logger = logging.getLogger('guillotina_elasticsearch_vacuum')


SELECT_BY_KEYS = '''SELECT zoid from objects where zoid = ANY($1)'''
BATCHED_GET_CHILDREN_BY_PARENT = """
SELECT zoid
FROM objects
WHERE of is NULL AND parent_id = ANY($1)
ORDER BY parent_id
LIMIT $2::int
OFFSET $3::int
"""

PAGE_SIZE = 1000

GET_OBS_BY_TID = f"""
SELECT zoid, tid
FROM objects
WHERE
    of is NULL AND
    tid >= $1
ORDER BY tid ASC, zoid ASC
LIMIT {PAGE_SIZE}
"""

GET_ALL_FOR_TID = f"""
SELECT zoid
FROM objects
WHERE
    of is NULL AND
    tid = $1 and zoid > $2
ORDER BY zoid ASC
LIMIT {PAGE_SIZE}
"""


async def clean_orphan_indexes(container):
    search = get_utility(ICatalogUtility)
    installed_indexes = await get_installed_sub_indexes(container)
    content_indexes = [val['index'] for val in
                       await get_content_sub_indexes(container)]
    for alias_name, index in installed_indexes.items():
        if alias_name not in content_indexes:
            # delete, no longer content available
            await search.conn.indices.close(alias_name)
            await search.conn.indices.delete_alias(index, alias_name)
            await search.conn.indices.delete(index)


class Vacuum:

    def __init__(self, txn, tm, request, container, last_tid=-2):
        self.txn = txn
        self.tm = tm
        self.request = request
        self.container = container
        self.orphaned = []
        self.missing = []
        self.utility = get_utility(ICatalogUtility)
        self.migrator = Migrator(
            self.utility, self.container, full=True, bulk_size=10)
        self.index_manager = get_adapter(self.container, IIndexManager)
        self.cache = LRU(200)
        self.last_tid = last_tid
        self.use_tid_query = True
        self.last_zoid = None
        # for state tracking so we get boundries right
        self.last_result_set = []

    async def iter_batched_es_keys(self):
        index_name = await self.index_manager.get_index_name()
        result = await self.utility.conn.search(
            index='{},{}__*'.format(index_name, index_name),
            scroll='15m',
            size=PAGE_SIZE,
            _source=False,
            body={
                "sort": ["_doc"]
            })
        yield [r['_id'] for r in result['hits']['hits']]
        scroll_id = result['_scroll_id']
        while scroll_id:
            try:
                result = await self.utility.conn.scroll(
                    scroll_id=scroll_id,
                    scroll='5m'
                )
            except aioelasticsearch.exceptions.TransportError:
                # no results
                break
            if len(result['hits']['hits']) == 0:
                break
            yield [r['_id'] for r in result['hits']['hits']]
            scroll_id = result['_scroll_id']

    async def get_db_page_of_keys(self, oids, page=1, page_size=PAGE_SIZE):
        conn = await self.txn.get_connection()
        clear_conn_statement_cache(conn)
        keys = []
        async with self.txn._lock:
            for record in await conn.fetch(
                    BATCHED_GET_CHILDREN_BY_PARENT, oids,
                    page_size, (page - 1) * page_size):
                keys.append(record['zoid'])
        return keys

    async def get_page_from_tid(self):
        conn = await self.txn.get_connection()
        clear_conn_statement_cache(conn)
        keys = []
        queried_tid = self.last_tid
        async with self.txn._lock:
            records = await conn.fetch(GET_OBS_BY_TID, queried_tid)
            for record in records:
                if record['zoid'] in (ROOT_ID, TRASHED_ID, self.container._p_oid):
                    continue
                keys.append(record['zoid'])
                self.last_tid = record['tid']
                self.last_zoid = record['zoid']
        if len(keys) == 0:
            if len(self.last_result_set) > 0:
                # now we have zero, increment, but only once
                self.last_tid = self.last_tid + 1
        self.last_result_set = keys
        return keys

    async def iter_paged_db_keys(self, oids):
        if self.use_tid_query:
            queried_tid = self.last_tid
            keys = await self.get_page_from_tid()
            while len(keys) > 0:
                yield keys
                if self.last_tid == queried_tid:
                    conn = await self.txn.get_connection()
                    logger.warning(f'Getting all keys from tid {self.last_tid}')
                    # we're stuck on same tid, get all for this tid
                    # and then move on...
                    clear_conn_statement_cache(conn)
                    records = await conn.fetch(
                        GET_ALL_FOR_TID, self.last_tid, self.last_zoid)
                    while len(records) > 0:
                        keys = []
                        for record in records:
                            if record['zoid'] in (ROOT_ID, TRASHED_ID, self.container._p_oid):
                                continue
                            keys.append(record['zoid'])
                            self.last_zoid = record['zoid']
                        yield keys
                        clear_conn_statement_cache(conn)
                        records = await conn.fetch(
                            GET_ALL_FOR_TID, self.last_tid, self.last_zoid)
                    self.last_tid = self.last_tid + 1
                queried_tid = self.last_tid
                keys = await self.get_page_from_tid()

        else:
            page_num = 1
            page = await self.get_db_page_of_keys(oids, page_num)
            while page:
                yield page
                async for sub_page in self.iter_paged_db_keys(page):
                    yield sub_page
                page_num += 1
                page = await self.get_db_page_of_keys(oids, page_num)

    async def get_object(self, oid):
        if oid in self.cache:
            return self.cache[oid]

        try:
            result = self.txn._manager._hard_cache.get(oid, None)
        except AttributeError:
            from guillotina.db.transaction import HARD_CACHE  # pylint: disable=E0611
            result = HARD_CACHE.get(oid, None)
        if result is None:
            clear_conn_statement_cache(await self.txn.get_connection())
            result = await self.txn._cache.get(oid=oid)

        if result is None:
            result = await self.tm._storage.load(self.txn, oid)

        obj = reader(result)
        obj._p_jar = self.txn
        if result['parent_id']:
            obj.__parent__ = await self.get_object(result['parent_id'])
        return obj

    async def process_missing(self, oid, full=True):
        # need to fill in parents in order for indexing to work...
        logger.warning(f'Index missing {oid}')
        try:
            obj = await self.get_object(oid)
        except KeyError:
            logger.warning(f'Could not find {oid}')
            return
        except (AttributeError, TypeError):
            logger.warning(f'Could not find {oid}', exc_info=True)
            return  # object or parent of object was removed, ignore
        await self.migrator.index_object(obj, full=full)

    async def setup(self):
        # how we're doing this...
        # 1) iterate through all es keys
        # 2) batch check obs exist in db
        # 3) iterate through all db keys
        # 4) batch check they are in elasticsearch
        # WHY this way?
        #   - uses less memory rather than getting all keys in both.
        #   - this way should allow us handle VERY large datasets

        self.index_name = await self.index_manager.get_index_name()
        self.migrator.work_index_name = self.index_name

    async def check_orphans(self):
        logger.warning(f'Checking orphans on container {self.container.id}', extra={
            'account': self.container.id
        })
        conn = await self.txn.get_connection()
        checked = 0
        async for es_batch in self.iter_batched_es_keys():
            checked += len(es_batch)
            clear_conn_statement_cache(conn)
            async with self.txn._lock:
                records = await conn.fetch(SELECT_BY_KEYS, es_batch)
            db_batch = set()
            for record in records:
                db_batch.add(record['zoid'])
            orphaned = [k for k in set(es_batch) - db_batch]
            if checked % 10000 == 0:
                logger.warning(f'Checked ophans: {checked}')
            if orphaned:
                # these are keys that are in ES but not in DB so we should
                # remove them..
                self.orphaned.extend(orphaned)
                logger.warning(f'deleting orphaned {len(orphaned)}')
                conn_es = await self.utility.conn.transport.get_connection()
                # delete by query for orphaned keys...
                index_path = '{},{}__*'.format(self.index_name, self.index_name)
                async with conn_es.session.post(
                        join(conn_es.base_url.human_repr(),
                             index_path, '_delete_by_query'),
                        headers={
                            'Content-Type': 'application/json'
                        },
                        data=json.dumps({
                            'query': {
                                'terms': {
                                    'uuid': orphaned
                                }
                            }
                        })) as resp:
                    pass

    async def check_missing(self):
        status = (f'Checking missing on container {self.container.id}, '
                  f'starting with TID: {self.last_tid}')
        logger.warning(status, extra={
            'account': self.container.id
        })
        conn = await self.txn.get_connection()
        containers = await conn.fetch(
            'select zoid from objects where parent_id = $1', ROOT_ID)
        if len(containers) > 1:
            # more than 1 container, we can't optimize by querying by tids
            self.use_tid_query = False

        checked = 0
        async for batch in self.iter_paged_db_keys([self.container._p_oid]):
            es_batch = []
            results = await self.utility.conn.search(
                '{},{}__*'.format(self.index_name, self.index_name), body={
                    'query': {
                        'terms': {
                            'uuid': batch
                        }
                    }
                },
                _source=False,
                size=PAGE_SIZE)
            for result in results['hits']['hits']:
                es_batch.append(result['_id'])
            missing = [k for k in set(batch) - set(es_batch)]
            checked += len(batch)
            status = f'Checked missing: {checked}: {self.last_tid}'
            logger.warning(status)
            if missing:
                logger.warning(
                    f'indexing missing: {len(missing)}, total checked: {checked}')
                # these are keys that are in DB but not in ES so we
                # should index them..
                self.missing.extend(missing)
                for oid in missing:
                    await self.process_missing(oid)

        await self.migrator.flush()
        await self.migrator.join_futures()


class VacuumCommand(Command):
    description = 'Run vacuum on elasticearch'
    vacuum_klass = Vacuum
    state = {}

    def get_parser(self):
        parser = super(VacuumCommand, self).get_parser()
        parser.add_argument(
            '--continuous', help='Continuously vacuum', action='store_true')
        parser.add_argument('--sleep', help='Time in seconds to sleep',
                            default=10 * 60, type=int)
        return parser

    async def run(self, arguments, settings, app):
        change_transaction_strategy('none')
        self.request._db_write_enabled = True
        self.request._message.headers['Host'] = 'localhost'
        await asyncio.gather(
            self.do_check(arguments, 'check_missing'),
            self.do_check(arguments, 'check_orphans'))

    async def do_check(self, arguments, check_name):
        first_run = True
        while arguments.continuous or first_run:
            if not first_run:
                await asyncio.sleep(arguments.sleep)
            else:
                first_run = False
            async for txn, tm, container in get_containers(self.request):
                try:
                    kwargs = {}
                    if container._p_oid in self.state:
                        kwargs = self.state[container._p_oid]
                    vacuum = self.vacuum_klass(
                        txn, tm, self.request, container, **kwargs)
                    await vacuum.setup()
                    func = getattr(vacuum, check_name)
                    await func()
                    if vacuum.last_tid > 0:
                        self.state[container._p_oid] = {
                            'last_tid': vacuum.last_tid
                        }
                    logger.warning(f'''Finished vacuuming with results:
Orphaned cleaned: {len(vacuum.orphaned)}
Missing added: {len(vacuum.missing)}
''')
                    await clean_orphan_indexes(container)
                except Exception:
                    logger.error('Error vacuuming', exc_info=True)
                finally:
                    await tm.abort(txn=txn)
