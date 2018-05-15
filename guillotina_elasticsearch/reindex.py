from guillotina.component import get_adapter
from guillotina.event import notify
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
            index_manager = get_adapter(self.request.container, IIndexManager)
        self.work_index_name = await index_manager.get_index_name()

        await notify(IndexProgress(
            self.request, self.context, 0, self.processed))
        await self.process_object(obj)
        await self.flush()
        await notify(IndexProgress(
            self.request, self.context, self.processed,
            self.processed, completed=True
        ))
