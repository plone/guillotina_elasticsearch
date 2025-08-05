from guillotina import task_vars
from guillotina.commands import Command
from guillotina.commands.utils import change_transaction_strategy
from guillotina.component import get_utility
from guillotina.interfaces import ICatalogUtility
from guillotina.tests.utils import get_mocked_request
from guillotina.tests.utils import login
from guillotina.utils import get_containers
from guillotina.utils import navigate_to
from guillotina_elasticsearch.reindex import Reindexer

import asyncio
import logging
import time


logger = logging.getLogger("guillotina_elasticsearch")


class printer:
    def write(self, txt):
        if isinstance(txt, bytes):
            txt = txt.decode("utf-8")
        logger.warning(txt.strip())


class ReindexCommand(Command):
    description = "Reindex"
    migrator = None
    reindexer = None

    def get_parser(self):
        parser = super(ReindexCommand, self).get_parser()
        parser.add_argument("--log-details", action="store_true")
        parser.add_argument("--memory-tracking", action="store_true")
        parser.add_argument("--reindex-security", action="store_true")
        parser.add_argument("--mapping-only", action="store_true")
        parser.add_argument("--container", help="Container to index")
        parser.add_argument("--path", help="Path of the container to index")
        return parser

    async def reindex_all(self, arguments):
        search = get_utility(ICatalogUtility)
        await asyncio.sleep(1)  # since something initialize custom types...
        async for _, tm, container in get_containers():
            try:
                index_container = True
                if arguments.container and container.id != arguments.container:
                    index_container = False
                if index_container:
                    self.reindexer = Reindexer(
                        search,
                        container,
                        response=printer(),
                        log_details=arguments.log_details,
                        memory_tracking=arguments.memory_tracking,
                        reindex_security=arguments.reindex_security,
                        mapping_only=arguments.mapping_only,
                        cache=False,
                    )
                    object_to_index = container
                    if arguments.path:
                        object_to_index = await navigate_to(container, arguments.path)
                    await self.reindexer.reindex(object_to_index)
                    seconds = int(time.time() - self.reindexer.start_time)
                    logger.warning(
                        f"""Finished reindex:
                        Total Seconds: {seconds}
                        Processed: {self.reindexer.processed}
                        Indexed: {self.reindexer.indexed}
                        Objects missing: {len(self.reindexer.missing)}
                        Objects orphaned: {len(self.reindexer.orphaned)}
                        """
                    )
            finally:
                await tm.commit()

    async def run(self, arguments, settings, app):
        request = get_mocked_request()
        login()
        task_vars.request.set(request)
        change_transaction_strategy("none")
        await self.reindex_all(arguments)
