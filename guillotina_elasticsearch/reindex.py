from guillotina.component import get_adapter
from guillotina.event import notify
from guillotina.utils import get_current_container
from guillotina_elasticsearch.events import IndexProgress
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.migration import Migrator


class Reindexer(Migrator):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.force = False
        if not self.reindex_security:
            self.full = True  # need to make sure we do one or the other

    async def reindex(self, obj):
        container = get_current_container()
        index_manager = get_adapter(container, IIndexManager)
        self.work_index_name = await index_manager.get_index_name()

        await notify(IndexProgress(self.context, 0, self.processed))
        await self.process_object(obj)
        await self.flush()

        await notify(
            IndexProgress(
                self.context,
                self.processed,
                self.processed,
                completed=True,
                request=self.request,
            )
        )
