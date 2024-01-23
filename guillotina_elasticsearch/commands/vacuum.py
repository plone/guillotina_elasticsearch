from elastic_transport import ObjectApiResponse
from elasticsearch import AsyncElasticsearch
from guillotina import task_vars
from guillotina.commands import Command
from guillotina.commands.utils import change_transaction_strategy
from guillotina.component import get_adapter
from guillotina.component import get_utility
from guillotina.db import ROOT_ID
from guillotina.db import TRASHED_ID
from guillotina.interfaces import ICatalogUtility
from guillotina.tests.utils import get_mocked_request
from guillotina.tests.utils import login
from guillotina.utils import get_containers
from guillotina.utils import get_object_by_uid
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.migration import Migrator
from lru import LRU  # pylint: disable=E0611

import asyncio
import elasticsearch
import logging


logger = logging.getLogger("guillotina_elasticsearch_vacuum")

GET_CONTAINERS = "select zoid from {objects_table} where parent_id = $1"
SELECT_BY_KEYS = f"""
SELECT zoid from {{objects_table}}
where zoid = ANY($1) AND parent_id != '{TRASHED_ID}'
"""
GET_CHILDREN_BY_PARENT = """
SELECT zoid, parent_id, tid
FROM {objects_table}
WHERE of is NULL AND parent_id = ANY($1)
ORDER BY parent_id
"""

PAGE_SIZE = 1000

GET_OBS_BY_TID = f"""
SELECT zoid, parent_id, tid
FROM {{objects_table}}
WHERE of is NULL and parent_id != '{TRASHED_ID}'
ORDER BY tid ASC, zoid ASC
"""

CREATE_INDEX = """
CREATE INDEX CONCURRENTLY IF NOT EXISTS
objects_tid_zoid ON {objects_table} (tid ASC, zoid ASC);"""


class Vacuum:
    def __init__(self, txn, tm, container, last_tid=-2):
        self.txn = txn
        self.tm = tm
        self.container = container
        self.orphaned = set()
        self.missing = set()
        self.out_of_date = set()
        self.utility = get_utility(ICatalogUtility)
        self.migrator = Migrator(self.utility, self.container, full=True, bulk_size=10)
        self.index_manager = get_adapter(self.container, IIndexManager)
        self.cache = LRU(200)
        self.last_tid = last_tid
        self.use_tid_query = True
        self.last_zoid = None
        # for state tracking so we get boundries right
        self.last_result_set = []
        self.conn: AsyncElasticsearch = self.utility.get_connection()

    def get_sql(self, source):
        storage = self.txn._manager._storage
        return source.format(objects_table=storage._objects_table_name)

    async def iter_batched_es_keys(self):
        # go through one index at a time...
        indexes = [self.index_name]
        for index_name in indexes:
            try:
                result: ObjectApiResponse = await self.conn.search(
                    index=index_name,
                    scroll="15m",
                    size=PAGE_SIZE,
                    _source=False,
                    body={"sort": ["_doc"]},
                )
            except elasticsearch.exceptions.NotFoundError:
                continue
            yield [r["_id"] for r in result["hits"]["hits"]], index_name
            scroll_id = result["_scroll_id"]
            while scroll_id:
                try:
                    result = await self.conn.scroll(scroll_id=scroll_id, scroll="5m")
                except elasticsearch.exceptions.TransportError:
                    # no results
                    break
                if len(result["hits"]["hits"]) == 0:
                    break
                yield [r["_id"] for r in result["hits"]["hits"]], index_name
                scroll_id = result["_scroll_id"]

    async def iter_paged_db_keys(self, oids):
        if self.use_tid_query:
            conn = await self.txn.get_connection()
            async with conn.transaction():
                sql = self.get_sql(GET_OBS_BY_TID)
                cur = await conn.cursor(sql)
                results = await cur.fetch(PAGE_SIZE)
                while len(results) > 0:
                    records = []
                    for record in results:
                        if record["zoid"] in (
                            ROOT_ID,
                            TRASHED_ID,
                            self.container.__uuid__,
                        ):
                            continue
                        records.append(record)
                        self.last_tid = record["tid"]
                        self.last_zoid = record["zoid"]
                    yield records
                    results = await cur.fetch(PAGE_SIZE)
        else:
            conn = await self.txn.get_connection()
            sql = self.get_sql(GET_CHILDREN_BY_PARENT)

            while oids:
                pos = 0
                new_oids = []
                while (pos * PAGE_SIZE) < len(oids):
                    async with conn.transaction():
                        cur = await conn.cursor(sql, oids[pos : pos + PAGE_SIZE])
                        pos += PAGE_SIZE
                        page = await cur.fetch(PAGE_SIZE)
                        while page:
                            yield page
                            new_oids.extend([r["zoid"] for r in page])
                            page = await cur.fetch(PAGE_SIZE)
                oids = new_oids

    async def get_object(self, oid):
        if oid in self.cache:
            return self.cache[oid]

        return await get_object_by_uid(oid)

    async def process_missing(self, oid, index_type="missing", folder=False):
        # need to fill in parents in order for indexing to work...
        logger.warning(f"Index {index_type} {oid}")
        try:
            obj = await self.get_object(oid)
        except (AttributeError, KeyError, TypeError, ModuleNotFoundError):
            logger.warning(f"Could not find {oid}")
            return  # object or parent of object was removed, ignore
        try:
            if folder:
                await self.migrator.process_object(obj)
            else:
                await self.migrator.index_object(obj)
        except TypeError:
            logger.warning(f"Could not index {oid}", exc_info=True)

    async def setup(self):
        # how we're doing this...
        # 1) iterate through all es keys
        # 2) batch check obs exist in db
        # 3) iterate through all db keys
        # 4) batch check they are in elasticsearch
        # WHY this way?
        #   - uses less memory rather than getting all keys in both.
        #   - this way should allow us handle VERY large datasets

        try:
            conn = await self.txn.get_connection()
            sql = self.get_sql(CREATE_INDEX)
            async with self.txn._lock:
                await conn.execute(sql)
        except Exception:
            pass

        self.index_name = await self.index_manager.get_index_name()
        self.migrator.work_index_name = self.index_name

    async def check_orphans(self):
        logger.warning(
            f"Checking orphans on container {self.container.id}",
            extra={"account": self.container.id},  # noqa
        )
        conn = await self.txn.get_connection()
        checked = 0
        async for es_batch, index_name in self.iter_batched_es_keys():
            checked += len(es_batch)
            async with self.txn._lock:
                sql = self.get_sql(SELECT_BY_KEYS)
                records = await conn.fetch(sql, es_batch)
            db_batch = set()
            for record in records:
                db_batch.add(record["zoid"])
            orphaned = [k for k in set(es_batch) - db_batch]
            if checked % 10000 == 0:
                logger.warning(f"Checked ophans: {checked}")
            if orphaned:
                # these are keys that are in ES but not in DB so we should
                # remove them..
                self.orphaned |= set(orphaned)
                logger.warning(f"deleting orphaned {len(orphaned)}")

                # delete by query for orphaned keys...
                data = await self.conn.delete_by_query(
                    index=index_name, body={"query": {"terms": {"_id": orphaned}}}
                )
                if data["deleted"] != len(orphaned):
                    logger.warning(
                        f'Was only able to clean up {len(data["deleted"])} '  # noqa
                        f"instead of {len(orphaned)}"
                    )

    async def check_missing(self):
        status = (
            f"Checking missing on container {self.container.id}, "
            f"starting with TID: {self.last_tid}"
        )
        logger.warning(status, extra={"account": self.container.id})
        conn = await self.txn.get_connection()
        sql = self.get_sql(GET_CONTAINERS)
        async with self.txn._lock:
            containers = await conn.fetch(sql, ROOT_ID)

        if len(containers) > 1:
            # more than 1 container, we can't optimize by querying by tids
            self.use_tid_query = False

        checked = 0
        async for batch in self.iter_paged_db_keys([self.container.__uuid__]):
            oids = [r["zoid"] for r in batch]
            try:
                results: ObjectApiResponse = await self.conn.search(
                    index=self.index_name,
                    body={"query": {"terms": {"uuid": oids}}},
                    _source=False,
                    fields=["tid", "parent_uuid"],
                    stored_fields="tid,parent_uuid",
                    size=PAGE_SIZE,
                )
            except elasticsearch.exceptions.NotFoundError:
                logger.warning(
                    f"Error searching index: {self.index_name}", exc_info=True
                )
                continue

            es_batch = {}
            for result in results["hits"]["hits"]:
                oid = result["_id"]
                tid = result.get("fields", {}).get("tid") or [-1]
                es_batch[oid] = {
                    "tid": int(tid[0]),
                    "parent_uuid": result.get("fields", {}).get(
                        "parent_uuid", ["_missing_"]
                    )[0],
                }
            for record in batch:
                oid = record["zoid"]
                tid = record["tid"]
                if oid == self.container.__uuid__:
                    continue
                if oid not in es_batch:
                    self.missing.add(oid)
                    await self.process_missing(oid)
                elif tid > es_batch[oid]["tid"] and es_batch[oid]["tid"] != -1:
                    self.out_of_date.add(oid)
                    await self.process_missing(oid, index_type="out of date")
                elif record["parent_id"] != es_batch[oid]["parent_uuid"]:
                    self.missing.add(oid)
                    await self.process_missing(oid, folder=True)

            checked += len(batch)
            logger.warning(
                f"Checked missing: {checked}: {self.last_tid}, "
                f"missing: {len(self.missing)}, out of date: {len(self.out_of_date)}"
            )  # noqa

        await self.migrator.flush()
        await self.migrator.join_futures()


class VacuumCommand(Command):
    description = "Run vacuum on elasticearch"
    vacuum_klass = Vacuum
    state = {}

    def get_parser(self):
        parser = super(VacuumCommand, self).get_parser()
        parser.add_argument(
            "--continuous", help="Continuously vacuum", action="store_true"
        )
        parser.add_argument(
            "--sleep", help="Time in seconds to sleep", default=10 * 60, type=int
        )
        return parser

    async def run(self, arguments, settings, app):
        request = get_mocked_request()
        login()
        task_vars.request.set(request)
        change_transaction_strategy("none")
        await asyncio.gather(
            self.do_check(arguments, "check_missing"),
            self.do_check(arguments, "check_orphans"),
        )

    async def do_check(self, arguments, check_name):
        first_run = True
        while arguments.continuous or first_run:
            if not first_run:
                await asyncio.sleep(arguments.sleep)
            else:
                first_run = False
            async for txn, tm, container in get_containers():
                try:
                    kwargs = {}
                    if container.__uuid__ in self.state:
                        kwargs = self.state[container.__uuid__]
                    vacuum = self.vacuum_klass(txn, tm, container, **kwargs)
                    await vacuum.setup()
                    func = getattr(vacuum, check_name)
                    await func()
                    if vacuum.last_tid > 0:
                        self.state[container.__uuid__] = {"last_tid": vacuum.last_tid}
                    logger.warning(
                        f"""Finished vacuuming with results:
Orphaned cleaned: {len(vacuum.orphaned)}
Missing added: {len(vacuum.missing)}
Out of date fixed: {len(vacuum.out_of_date)}
"""
                    )
                except Exception:
                    logger.error("Error vacuuming", exc_info=True)
                finally:
                    await tm.abort(txn=txn)
