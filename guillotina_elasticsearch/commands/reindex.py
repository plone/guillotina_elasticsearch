from guillotina.commands import Command
from guillotina.component import getUtility
from guillotina.interfaces import IApplication
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IDatabase


class ReindexCommand(Command):
    description = 'Reindex all containers'

    def get_parser(self):
        parser = super(ReindexCommand, self).get_parser()
        parser.add_argument('--clean', help='Clear ES before indexing',
                            action='store_true')
        parser.add_argument('--resume', action='store_true')

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
        options = dict(clean=False, update=True)
        if arguments.clean:
            options = dict(clean=True)
        elif arguments.resume:
            options = dict(clean=False, update_missing=True)
        search = getUtility(ICatalogUtility)
        async for tm, container in self.get_containers():
            await search.reindex_all_content(container, **options)
