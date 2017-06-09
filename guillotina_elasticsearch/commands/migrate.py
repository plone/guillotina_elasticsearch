from guillotina.commands import Command
from guillotina.component import getUtility
from guillotina.interfaces import IApplication
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IDatabase
from guillotina_elasticsearch.migration import Migrator

import time
import logging


logger = logging.getLogger('guillotina_elasticsearch')


class printer:
    def write(self, txt):
        if isinstance(txt, bytes):
            txt = txt.decode('utf-8')
        logger.warning(txt.strip())


class MigrateCommand(Command):
    description = 'Migrate indexes'
    migrator = None

    def get_parser(self):
        parser = super(MigrateCommand, self).get_parser()
        parser.add_argument('--full', help='Do a full reindex', action='store_true')
        parser.add_argument('--force', help='Override failing migration if existing '
                                            'migration index exists',
                            action='store_true')
        parser.add_argument('--log-details', action='store_true')
        parser.add_argument('--memory-tracking', action='store_true')
        parser.add_argument('--reindex-security', action='store_true')
        parser.add_argument('--mapping-only', action='store_true')
        return parser

    async def get_containers(self):
        root = getUtility(IApplication, name='root')
        for _id, db in root:
            if IDatabase.providedBy(db):
                db._db._storage._transaction_strategy = 'none'
                tm = db.get_transaction_manager()
                tm.request = self.request
                await tm.begin(self.request)
                async for s_id, container in db.async_items():
                    tm.request.container = container
                    yield tm, container

    async def migrate_all(self, arguments):
        search = getUtility(ICatalogUtility)
        async for tm, container in self.get_containers():
            self.migrator = Migrator(
                search, container, response=printer(), full=arguments.full,
                force=arguments.force, log_details=arguments.log_details,
                memory_tracking=arguments.memory_tracking,
                reindex_security=arguments.reindex_security,
                mapping_only=arguments.mapping_only)
            await self.migrator.run_migration()
            seconds = int(time.time() - self.migrator.start_time)
            logger.warning(f'''Finished migration:
Total Seconds: {seconds}
Processed: {self.migrator.processed}
Indexed: {self.migrator.indexed}
Objects missing: {len(self.migrator.missing)}
Objects orphaned: {len(self.migrator.orphaned)}
Mapping Diff: {self.migrator.mapping_diff}
''')

    def run(self, arguments, settings, app):
        loop = self.get_loop()
        try:
            loop.run_until_complete(self.migrate_all(arguments))
        except KeyboardInterrupt:  # pragma: no cover
            pass
        finally:
            if self.migrator.status != 'done':
                loop = self.get_loop()
                loop.run_until_complete(self.migrator.cancel_migration())
