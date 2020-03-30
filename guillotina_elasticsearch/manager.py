from guillotina import app_settings
from guillotina import configure
from guillotina import task_vars
from guillotina.annotations import AnnotationData
from guillotina.catalog.index import index_object
from guillotina.component import get_adapter
from guillotina.component import query_utility
from guillotina.db.transaction import Status
from guillotina.db.uid import get_short_uid
from guillotina.directives import index_field
from guillotina.exceptions import NoIndexField
from guillotina.interfaces import IAnnotations
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IContainer
from guillotina.interfaces import IObjectAddedEvent
from guillotina.interfaces import IResource
from guillotina.transactions import get_transaction
from guillotina.transactions import transaction
from guillotina.utils import execute
from guillotina.utils import get_registry as guillotina_get_registry
from guillotina.utils import resolve_dotted_name
from guillotina_elasticsearch.directives import index
from guillotina_elasticsearch.interfaces import IContentIndex
from guillotina_elasticsearch.interfaces import IIndexActive
from guillotina_elasticsearch.interfaces import IIndexManager
from guillotina_elasticsearch.interfaces import SUB_INDEX_SEPERATOR
from guillotina_elasticsearch.schema import get_mappings
from guillotina_elasticsearch.utils import get_migration_lock
from zope.interface import alsoProvides
from zope.interface.interface import TAGGED_DATA

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


@configure.adapter(for_=IContentIndex, provides=IIndexManager)
class ContentIndexManager(ContainerIndexManager):
    """
    Custom index manager which uses a different index from global
    """

    def __init__(self, ob):
        super().__init__(ob)
        self.object_settings = None

    async def get_registry(self, refresh=False):
        if refresh and self.object_settings is not None:
            txn = get_transaction()
            await txn.refresh(self.object_settings)
        if self.object_settings is None:
            txn = get_transaction()
            is_active = txn.status in (Status.ACTIVE, Status.COMMITTING)
            if is_active:
                self.object_settings = await self._get_registry_or_create()
            else:
                async with transaction():
                    self.object_settings = await self._get_registry_or_create()
        return self.object_settings

    async def _get_registry_or_create(self):
        annotations_container = IAnnotations(self.context)
        object_settings = await annotations_container.async_get("default")  # noqa
        if object_settings is None:
            # need to create annotation...
            object_settings = AnnotationData()
            await annotations_container.async_set("default", object_settings)
        return object_settings

    def _generate_new_index_name(self):
        """
        index name structure is:
        - {settings-prefix}{container id}__{type}-{short uid}
        """
        container_name = super()._generate_new_index_name()
        return "{}{}{}-{}".format(
            container_name,
            SUB_INDEX_SEPERATOR,
            self.context.type_name.lower(),
            get_short_uid(self.context.__uuid__),
        )

    def _get_index_name(self, index_name, version):
        return str(version) + "_" + index_name

    async def get_index_settings(self):
        index_settings = await super().get_index_settings()
        index_data = getattr(type(self.context), TAGGED_DATA, {}).get(index.key)
        if index_data is not None:
            index_settings.update(index_data.get("settings", {}))
        return index_settings

    async def get_mappings(self):
        schemas = await self.get_schemas()
        if schemas is not None:
            return get_mappings(schemas)
        return get_mappings()

    async def get_schemas(self):
        index_data = getattr(type(self.context), TAGGED_DATA, {}).get(index.key)
        if index_data and "schemas" in index_data:
            schemas = [IResource]  # require basic index fields on everything
            schemas.extend(
                [resolve_dotted_name(s) for s in index_data.get("schemas") or []]
            )
            return set(schemas)

    async def finish_migration(self):
        await super().finish_migration()
        await index_object(self.context, indexes=["elastic_index"], modified=True)


async def _teardown_failed_request_with_index(im):
    utility = query_utility(ICatalogUtility)
    if utility:
        await utility._delete_index(im)


# make sure it is run before indexers
@configure.subscriber(for_=(IContentIndex, IObjectAddedEvent), priority=0)
async def init_index(context, subscriber):
    try:
        im = get_adapter(context, IIndexManager)
        utility = query_utility(ICatalogUtility)

        if utility is None:
            return
        index_name = await im.get_index_name()
        real_index_name = await im.get_real_index_name()

        conn = utility.get_connection()

        await utility.create_index(real_index_name, im)
        await conn.indices.put_alias(name=index_name, index=real_index_name)
        await conn.indices.close(real_index_name)

        await conn.indices.open(real_index_name)

        await conn.cluster.health(wait_for_status="yellow")
        alsoProvides(context, IIndexActive)

        execute.add_future(
            "cleanup-" + context.uuid,
            _teardown_failed_request_with_index,
            scope="failure",
            args=[im],
        )
    except Exception:
        logger.error("Error creating index for content", exc_info=True)
        raise


@index_field.with_accessor(
    IResource, "elastic_index", type="keyword", store=True, fields=["elastic_index"]
)
async def elastic_index_field(ob):
    if IIndexActive.providedBy(ob):
        im = get_adapter(ob, IIndexManager)
        return await im.get_index_name()
    raise NoIndexField
