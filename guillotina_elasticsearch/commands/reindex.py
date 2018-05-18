from guillotina.commands import Command
from guillotina.commands.utils import change_transaction_strategy
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.utils import get_containers
from guillotina_elasticsearch.reindex import Reindexer

import asyncio
import logging
import time


logger = logging.getLogger('guillotina_elasticsearch')


class printer:
    def write(self, txt):
        if isinstance(txt, bytes):
            txt = txt.decode('utf-8')
        logger.warning(txt.strip())


class ReindexCommand(Command):
    description = 'Reindex'
    migrator = None
    reindexer = None

    def get_parser(self):
        parser = super(ReindexCommand, self).get_parser()
        parser.add_argument('--log-details', action='store_true')
        parser.add_argument('--memory-tracking', action='store_true')
        parser.add_argument('--reindex-security', action='store_true')
        parser.add_argument('--mapping-only', action='store_true')
        return parser

    async def reindex_all(self, arguments):
        search = get_utility(ICatalogUtility)
        await asyncio.sleep(1)  # since something initialize custom types...
        async for _, tm, container in get_containers(self.request):
            try:
                self.reindexer = Reindexer(
                    search, container, response=printer(),
                    log_details=arguments.log_details,
                    memory_tracking=arguments.memory_tracking,
                    reindex_security=arguments.reindex_security,
                    mapping_only=arguments.mapping_only)
                await self.reindexer.reindex(container)
                seconds = int(time.time() - self.reindexer.start_time)
                logger.warning(f'''Finished reindex:
Total Seconds: {seconds}
Processed: {self.reindexer.processed}
Indexed: {self.reindexer.indexed}
Objects missing: {len(self.reindexer.missing)}
Objects orphaned: {len(self.reindexer.orphaned)}
''')
            finally:
                await tm.commit(self.request)

    def run(self, arguments, settings, app):
        change_transaction_strategy('none')
        loop = self.get_loop()
        loop.run_until_complete(self.reindex_all(arguments))
