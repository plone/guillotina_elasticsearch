from guillotina_elasticsearch.migration import Migrator


class Reindexer(Migrator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.force = False
        if not self.reindex_security:
            self.full = True  # need to make sure we do one or the other

    async def reindex(self, obj):
        self.work_index_name = await self.utility.get_index_name(self.request.container)
        await self.process_object(obj)
        await self.flush()
