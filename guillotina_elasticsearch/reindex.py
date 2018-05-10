from guillotina_elasticsearch.migration import Migrator
from guillotina_elasticsearch.events import IndexProgress
from guillotina.event import notify


class Reindexer(Migrator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.force = False
        if not self.reindex_security:
            self.full = True  # need to make sure we do one or the other

    async def reindex(self, obj):
        self.work_index_name = await self.utility.get_index_name(self.request.container)

        await notify(IndexProgress(
            self.request, self.context, 0, self.processed))
        await self.process_object(obj)
        await self.flush()
        await notify(IndexProgress(
            self.request, self.context, self.processed,
            self.processed, completed=True
        ))
