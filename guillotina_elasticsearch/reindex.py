from guillotina.component import get_adapter
from guillotina.event import notify
from guillotina.utils import get_current_container
from guillotina_elasticsearch.events import IndexProgress
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.migration import Migrator
from guillotina_elasticsearch.utils import find_index_manager


class Reindexer(Migrator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.force = False
        if not self.reindex_security:
            self.full = True  # need to make sure we do one or the other

    async def reindex(self, obj):
        index_manager = find_index_manager(obj)
        if index_manager is None:
            container = get_current_container()
            index_manager = get_adapter(container, IIndexManager)
        self.work_index_name = await index_manager.get_index_name()

        await notify(IndexProgress(self.context, 0, self.processed))
        await self.process_object(obj)
        await self.flush()
        if len(self.sub_indexes) > 0:
            # could cause sub indexes to need to be run through as well.
            for ob in self.sub_indexes:
                im = get_adapter(ob, IIndexManager)
                reindexer = Reindexer(
                    self.utility,
                    ob,
                    response=self.response,
                    force=self.force,
                    log_details=self.log_details,
                    memory_tracking=self.memory_tracking,
                    bulk_size=self.bulk_size,
                    full=self.full,
                    reindex_security=self.reindex_security,
                    mapping_only=self.mapping_only,
                    index_manager=im,
                    request=self.request,
                )
                reindexer.processed = self.processed
                reindexer.work_index_name = await im.get_index_name()
                await reindexer.process_folder(ob)
                await reindexer.flush()
                self.processed = reindexer.processed

        await notify(
            IndexProgress(
                self.context,
                self.processed,
                self.processed,
                completed=True,
                request=self.request,
            )
        )
