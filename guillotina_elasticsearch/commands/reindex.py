from guillotina.commands import Command
from guillotina.component import getUtility
from guillotina.interfaces import IApplication
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IDatabase
from guillotina_elasticsearch.reindex import Reindexer


class printer:
    def write(self, txt):
        if isinstance(txt, bytes):
            txt = txt.decode('utf-8')
        print(txt.strip())


class ReindexCommand(Command):
    description = 'Reindex all containers'

    def get_parser(self):
        parser = super(ReindexCommand, self).get_parser()
        parser.add_argument('--clean', help='Clear ES before indexing',
                            action='store_true')
        parser.add_argument('--security', action='store_true')
        parser.add_argument('--update', action='store_true')
        parser.add_argument('--update-missing', action='store_true')
        parser.add_argument('--log-details', action='store_true')
        parser.add_argument('--memory-tracking', action='store_true')
        return parser

    async def get_containers(self):
        root = getUtility(IApplication, name='root')
        for _id, db in root:
            if IDatabase.providedBy(db):
                tm = db.get_transaction_manager()
                tm.request = self.request
                await tm.begin(self.request)
                async for s_id, container in db.async_items():
                    tm.request.container = container
                    yield tm, container

    async def run(self, arguments, settings, app):
        search = getUtility(ICatalogUtility)
        async for tm, container in self.get_containers():
            reindexer = Reindexer(
                search, container, security=arguments.security, response=printer(),
                clean=arguments.clean, update=arguments.update,
                update_missing=arguments.update_missing,
                log_details=arguments.log_details,
                memory_tracking=arguments.memory_tracking)
            await reindexer.all_content()
            print('Finished reindexing in {} seconds'.format(
                int(reindexer.counter.per_sec())))
