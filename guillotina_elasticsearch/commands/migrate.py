from guillotina.commands import Command
from guillotina.component import getUtility
from guillotina.interfaces import IApplication
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IDatabase
from guillotina_elasticsearch.migration import Migrator

import time


class printer:
    def write(self, txt):
        if isinstance(txt, bytes):
            txt = txt.decode('utf-8')
        print(txt.strip())


class ReindexCommand(Command):
    description = 'Migrate indexes'

    def get_parser(self):
        parser = super(ReindexCommand, self).get_parser()
        parser.add_argument('--full', help='Do a full reindex', action='store_true')
        parser.add_argument('--force', help='Override failing if existin migration index exists',
                            action='store_true')
        parser.add_argument('--log-details', action='store_true')
        parser.add_argument('--memory-tracking', action='store_true')
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

    async def run(self, arguments, settings, app):
        search = getUtility(ICatalogUtility)
        async for tm, container in self.get_containers():
            migrator = Migrator(
                search, container, response=printer(), full=arguments.full,
                force=arguments.force, log_details=arguments.log_details,
                memory_tracking=arguments.memory_tracking)
            await migrator.run_migration()
            seconds = int(time.time() - migrator.start_time)
            print(f'''Finished reindexing:
Total Seconds: {seconds}
Indexed: {migrator.indexed}
Objects missing: {len(migrator.missing)}
Objects orphaned: {len(migrator.orphaned)}
''')
