from guillotina import app_settings
from guillotina import configure
from guillotina import task_vars
from guillotina.db.transaction import Status
from guillotina.interfaces import IContainer
from guillotina.transactions import get_transaction
from guillotina.transactions import transaction
from guillotina.utils import get_registry as guillotina_get_registry
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.schema import get_mappings
from guillotina_elasticsearch.utils import get_migration_lock

import logging


logger = logging.getLogger("guillotina_elasticsearch")


def default_settings():
    return {
        "analysis": {
            "analyzer": {"path_analyzer": {"tokenizer": "path_tokenizer"}},
            "tokenizer": {
                "path_tokenizer": {"type": "path_hierarchy", "delimiter": "/"}
            },
            "filter": {},
            "char_filter": {},
        }
    }


@configure.adapter(for_=IContainer, provides=IIndexManager)
class ContainerIndexManager:
    """
    Default index manager which uses the global index
    """

    def __init__(self, ob):
        self.container = task_vars.container.get()
        self.db = task_vars.db.get()
        self.context = ob

    async def get_indexes(self):
        indexes = []
        index_name = await self.get_real_index_name()
        indexes.append(index_name)
        # also need to call on next index while it's running...
        async with get_migration_lock(index_name):
            next_index_name = await self.get_migration_index_name()
        if next_index_name:
            indexes.append(next_index_name)
        return indexes

    async def get_index_settings(self):
        index_settings = default_settings()
        index_settings.update(app_settings.get("elasticsearch", {}).get("index", {}))
        return index_settings

    async def get_mappings(self):
        return get_mappings()

    def _generate_new_index_name(self):
        return "{}{}-{}".format(
            app_settings["elasticsearch"].get("index_name_prefix", "guillotina-"),
            self.db.id,
            self.container.id,
        )

    def _get_index_name(self, index_name, version):
        return index_name + "_" + str(version)

    async def get_index_name(self):
        registry = await self.get_registry()

        try:
            result = registry["el_index_name"]
        except KeyError:
            txn = get_transaction()
            is_active = txn.status in (Status.ACTIVE, Status.COMMITTING)
            if is_active:
                result = self._generate_new_index_name()
                registry["el_index_name"] = result
                registry.register()
            else:
                async with transaction():
                    result = self._generate_new_index_name()
                    registry["el_index_name"] = result
                    registry.register()
        return result

    async def get_real_index_name(self):
        index_name = await self.get_index_name()
        version = await self._get_version()
        return self._get_index_name(index_name, version)

    async def get_migration_index_name(self):
        registry = await self.get_registry()
        if (
            "el_next_index_version" not in registry
            or registry["el_next_index_version"] is None
        ):
            return None
        index_name = await self.get_index_name()
        version = registry["el_next_index_version"]
        return self._get_index_name(index_name, version)

    async def start_migration(self):
        version = await self._get_version()
        next_version = version + 1
        index_name = await self.get_index_name()
        registry = await self.get_registry()
        migration_index_name = self._get_index_name(index_name, next_version)
        registry["el_next_index_version"] = next_version
        registry.register()
        return migration_index_name

    async def finish_migration(self):
        registry = await self.get_registry()
        next_version = registry["el_next_index_version"]
        assert next_version is not None
        txn = get_transaction()
        await txn.refresh(registry)
        registry["el_index_version"] = next_version
        registry["el_next_index_version"] = None
        registry.register()

    async def _get_version(self):
        registry = await self.get_registry()
        try:
            version = registry["el_index_version"]
        except KeyError:
            version = 1
        return version

    async def get_registry(self):
        registry = await guillotina_get_registry(self.context)
        return registry

    async def cancel_migration(self):
        registry = await self.get_registry()
        registry["el_next_index_version"] = None
        registry.register()

    async def get_schemas(self):
        pass
