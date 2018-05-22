from copy import deepcopy
from guillotina import app_settings
from guillotina import configure
from guillotina.annotations import AnnotationData
from guillotina.component import get_adapter
from guillotina.component import get_utility
from guillotina.db.oid import get_short_oid
from guillotina.directives import index_field
from guillotina.exceptions import NoIndexField
from guillotina.interfaces import IAnnotations
from guillotina.interfaces import ICatalogUtility
from guillotina.interfaces import IContainer
from guillotina.interfaces import IObjectAddedEvent
from guillotina.interfaces import IResource
from guillotina.registry import REGISTRY_DATA_KEY
from guillotina.transactions import get_transaction
from guillotina.utils import get_current_request
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


logger = logging.getLogger('guillotina_elasticsearch')


DEFAULT_SETTINGS = {
    "analysis": {
        "analyzer": {
            "path_analyzer": {
                "tokenizer": "path_tokenizer"
            }
        },
        "tokenizer": {
            "path_tokenizer": {
                "type": "path_hierarchy",
                "delimiter": "/"
            }
        },
        "filter": {
        },
        "char_filter": {
        }
    },
    'index.mapper.dynamic': False
}


@configure.adapter(
    for_=IContainer,
    provides=IIndexManager)
class ContainerIndexManager:
    '''
    Default index manager which uses the global index
    '''

    def __init__(self, ob, request=None):
        if request is None:
            request = get_current_request()
        self.request = get_current_request()
        self.context = ob
        if hasattr(self.request, 'container'):
            self.container = self.request.container
        else:
            if IContainer.providedBy(ob):
                self.container = ob
            else:
                raise Exception('Could not location container object')

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
        index_settings = deepcopy(DEFAULT_SETTINGS)
        index_settings.update(app_settings.get('index', {}))
        return index_settings

    async def get_mappings(self):
        return get_mappings()

    def _generate_new_index_name(self):
        return '{}{}-{}'.format(
            app_settings['elasticsearch'].get(
                'index_name_prefix', 'guillotina-'),
            self.request._db_id,
            self.container.id)

    def _get_index_name(self, index_name, version):
        return index_name + '_' + str(version)

    async def get_index_name(self):
        registry = await self._get_registry()

        try:
            result = registry['el_index_name']
        except KeyError:
            result = self._generate_new_index_name()
            registry['el_index_name'] = result
            registry._p_register()
        return result

    async def get_real_index_name(self):
        index_name = await self.get_index_name()
        version = await self._get_version()
        return self._get_index_name(index_name, version)

    async def get_migration_index_name(self):
        registry = await self._get_registry()
        if ('el_next_index_version' not in registry or
                registry['el_next_index_version'] is None):
            return None
        index_name = await self.get_index_name()
        version = registry['el_next_index_version']
        return self._get_index_name(index_name, version)

    async def start_migration(self):
        version = await self._get_version()
        next_version = version + 1
        index_name = await self.get_index_name()
        registry = await self._get_registry()
        migration_index_name = self._get_index_name(index_name, next_version)
        registry['el_next_index_version'] = next_version
        registry._p_register()
        return migration_index_name

    async def finish_migration(self):
        registry = await self._get_registry(refresh=True)
        assert registry['el_next_index_version'] is not None
        registry['el_index_version'] = registry['el_next_index_version']
        registry['el_next_index_version'] = None
        registry._p_register()

    async def _get_version(self):
        registry = await self._get_registry()
        try:
            version = registry['el_index_version']
        except KeyError:
            version = 1
        return version

    async def _get_registry(self, refresh=False):
        if (refresh and hasattr(self.request, 'container_settings') and
                REGISTRY_DATA_KEY in self.container.__annotations__):
            txn = get_transaction(self.request)
            await txn.refresh(self.request.container_settings)
        if hasattr(self.request, 'container_settings'):
            return self.request.container_settings
        annotations_container = IAnnotations(self.container)
        self.request.container_settings = await annotations_container.async_get(REGISTRY_DATA_KEY)
        return self.request.container_settings

    async def cancel_migration(self):
        registry = await self._get_registry()
        registry['el_next_index_version'] = None
        registry._p_register()

    async def get_schemas(self):
        pass


@configure.adapter(
    for_=IContentIndex,
    provides=IIndexManager)
class ContentIndexManager(ContainerIndexManager):
    '''
    Custom index manager which uses a different index from global
    '''

    def __init__(self, ob, request=None):
        super().__init__(ob, request=request)
        self.object_settings = None

    async def _get_registry(self, refresh=False):
        if (refresh and self.object_settings is not None):
            txn = get_transaction(self.request)
            await txn.refresh(self.object_settings)
        annotations_container = IAnnotations(self.context)
        self.object_settings = await annotations_container.async_get('default')
        if self.object_settings is None:
            # need to create annotation...
            self.object_settings = AnnotationData()
            await annotations_container.async_set('default', self.object_settings)
        return self.object_settings

    def _generate_new_index_name(self):
        '''
        index name structure is:
        - {settings-prefix}{container id}__{type}-{short uid}
        '''
        container_name = super()._generate_new_index_name()
        return '{}{}{}-{}'.format(
            container_name, SUB_INDEX_SEPERATOR,
            self.context.type_name.lower(), get_short_oid(self.context._p_oid)
        )

    def _get_index_name(self, index_name, version):
        return str(version) + '_' + index_name

    async def get_index_settings(self):
        index_settings = await super().get_index_settings()
        index_data = getattr(
            type(self.context), TAGGED_DATA, {}).get(index.key)
        if index_data is not None:
            index_settings.update(index_data.get('settings', {}))
        return index_settings

    async def get_mappings(self):
        schemas = await self.get_schemas()
        if schemas is not None:
            return get_mappings(schemas)
        return get_mappings()

    async def get_schemas(self):
        index_data = getattr(
            type(self.context), TAGGED_DATA, {}).get(index.key)
        if index_data and 'schemas' in index_data:
            schemas = [IResource]  # require basic index fields on everything...
            schemas.extend(
                [resolve_dotted_name(s) for s in
                 index_data.get('schemas') or []])
            return set(schemas)


async def _teardown_failed_request_with_index(im):
    utility = get_utility(ICatalogUtility)
    await utility._delete_index(im)


# make sure it is run before indexers
@configure.subscriber(
    for_=(IContentIndex, IObjectAddedEvent), priority=0)
async def init_index(context, subscriber):
    try:
        im = get_adapter(context, IIndexManager)
        utility = get_utility(ICatalogUtility)

        index_name = await im.get_index_name()
        real_index_name = await im.get_real_index_name()

        await utility.create_index(real_index_name, im)
        await utility.conn.indices.put_alias(
            name=index_name, index=real_index_name)

        await utility.conn.cluster.health(wait_for_status='yellow')  # pylint: disable=E1123

        alsoProvides(context, IIndexActive)

        request = get_current_request()
        request.add_future(
            'cleanup-' + context.uuid,
            _teardown_failed_request_with_index, scope='failure',
            args=[im])
    except Exception:
        logger.error('Error creating index for content', exc_info=True)
        raise


@index_field.with_accessor(
    IResource, 'elastic_index', type='keyword', store=True, fields=['elastic_index'])
async def elastic_index_field(ob):
    if IIndexActive.providedBy(ob):
        im = get_adapter(ob, IIndexManager)
        return await im.get_index_name()
    raise NoIndexField
