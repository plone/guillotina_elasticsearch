from guillotina.commands import Command
from guillotina.component import getUtility
from guillotina.db.reader import reader
from guillotina.db.transaction import HARD_CACHE
from guillotina.interfaces import ICatalogUtility
from guillotina.utils import get_containers
from guillotina_elasticsearch.migration import Migrator
from lru import LRU

import asyncio
import json
import logging


logger = logging.getLogger('guillotina_elasticsearch')


SELECT_BY_KEYS = '''SELECT zoid from objects where zoid = ANY($1)'''
BATCHED_GET_CHILDREN_BY_PARENT = """
SELECT zoid
FROM objects
WHERE parent_id = ANY($1)
ORDER BY zoid
LIMIT $2::int
OFFSET $3::int
"""


class Vacuum:

    def __init__(self, txn, tm, request, container):
        self.txn = txn
        self.tm = tm
        self.request = request
        self.container = container
        self.orphaned = []
        self.missing = []
        self.utility = getUtility(ICatalogUtility)
        self.migrator = Migrator(self.utility, self.container, full=True)
        self.cache = LRU(200)

    async def iter_batched_es_keys(self):
        page_size = 3000
        index_name = await self.utility.get_index_name(self.container)
        result = await self.utility.conn.search(
            index=index_name,
            scroll='2m',
            size=page_size,
            stored_fields='',
            body={
                "sort": ["_doc"]
            })
        yield [r['_id'] for r in result['hits']['hits']]
        scroll_id = result['_scroll_id']
        while scroll_id:
            result = await self.utility.conn.scroll(
                scroll_id=scroll_id,
                scroll='2m'
            )
            if len(result['hits']['hits']) == 0:
                break
            yield [r['_id'] for r in result['hits']['hits']]
            scroll_id = result['_scroll_id']

    async def get_db_page_of_keys(self, oids, page=1, page_size=1000):
        conn = self.txn._manager._storage._read_conn
        smt = await conn.prepare(BATCHED_GET_CHILDREN_BY_PARENT)
        keys = []
        for record in await smt.fetch(oids, page_size, (page - 1) * page_size):
            keys.append(record['zoid'])
        return keys

    async def iter_paged_db_keys(self, oids):
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

        result = HARD_CACHE.get(oid, None)
        if result is None:
            result = await self.txn._cache.get(oid=oid)

        if result is None:
            result = await self.tm._storage.load(self.txn, oid)

        obj = reader(result)
        obj._p_jar = self.txn
        if result['parent_id']:
            obj.__parent__ = await self.get_object(result['parent_id'])
        return obj

    async def process_missing(self, oid):
        # need to fill in parents in order for indexing to work...
        try:
            obj = await self.get_object(oid)
        except KeyError:
            return  # object or parent of object was removed, ignore
        await self.migrator.index_object(obj, full=True)

    async def __call__(self):
        # how we're doing this...
        # 1) iterate through all es keys
        # 2) batch check obs exist in db
        # 3) iterate through all db keys
        # 4) batch check they are in elasticsearch
        # WHY this way?
        #   - uses less memory rather than getting all keys in both.
        #   - this way should allow us handle VERY large datasets

        index_name = await self.utility.get_index_name(self.container)
        self.migrator.work_index_name = index_name

        conn = self.txn._manager._storage._read_conn
        smt = await conn.prepare(SELECT_BY_KEYS)
        logger.warn('Checking orphaned elasticsearch entries')
        async for es_batch in self.iter_batched_es_keys():
            records = await smt.fetch(es_batch)
            db_batch = set()
            for record in records:
                db_batch.add(record['zoid'])
            orphaned = [k for k in (set(es_batch) - db_batch)]
            if len(orphaned) > 0:
                # these are keys that are in ES but not in DB so we should remove them..
                self.orphaned.extend(orphaned)
                logger.warn(f'deleting orphaned {len(orphaned)}')
                conn_es = await self.utility.conn.transport.get_connection()
                # delete by query for orphaned keys...
                await conn_es._session.post(
                    conn_es._base_url.human_repr() + index_name + '/_delete_by_query',
                    data=json.dumps({
                        'query': {
                            'terms': {
                                'uuid': orphaned
                            }
                        }
                    }))

        logger.warn('Checking missing elasticsearch entries')
        async for batch in self.iter_paged_db_keys([self.container._p_oid]):
            es_batch = []
            results = await self.utility.conn.search(
                index_name, body={
                    'query': {
                        'terms': {
                            'uuid': batch
                        }
                    }
                },
                stored_fields='',
                size=3000)
            for result in results['hits']['hits']:
                es_batch.append(result['_id'])
            missing = [k for k in (set(batch) - set(es_batch))]
            if len(missing) > 0:
                logger.warn(f'indexing missing {len(missing)}')
                # these are keys that are in DB but not in ES so we should index them..
                self.missing.extend(missing)
                batch = []
                for oid in missing:
                    batch.append(self.process_missing(oid))
                    if len(batch) >= 10:
                        await asyncio.gather(*batch)
                        batch = []
                await asyncio.gather(*batch)

        await self.migrator.flush()


class VacuumCommand(Command):
    '''
    '''
    description = 'Run vacuum on elasticearch'

    def get_parser(self):
        parser = super(VacuumCommand, self).get_parser()
        parser.add_argument('--continuous', help='Continuously vacuum', action='store_true')
        parser.add_argument('--sleep', help='Time in seconds to sleep',
                            default=300, type=int)
        return parser

    async def run(self, arguments, settings, app):
        self.request._db_write_enabled = True
        self.request._message.headers['Host'] = 'localhost'
        first_run = True
        while arguments.continuous or first_run:
            if not first_run:
                await asyncio.sleep(arguments.sleep)
            else:
                first_run = False
            async for txn, tm, container in get_containers(self.request):
                logger.warn(f'Vacuuming container {container.id}', extra={
                    'account': container.id
                })
                try:
                    vacuum = Vacuum(txn, tm, self.request, container)
                    await vacuum()
                    logger.warn(f'''Finished vacuuming with results:
    Orphaned cleaned: {len(vacuum.orphaned)}
    Missing added: {len(vacuum.missing)}
    ''')
                except:
                    logger.error('Error vacuuming', exc_info=True)
