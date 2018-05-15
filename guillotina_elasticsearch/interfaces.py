from guillotina.interfaces import ICatalogUtility
from zope.interface import Interface


try:
    from guillotina.async_util import IAsyncUtility
except ImportError:
    from guillotina.async import IAsyncUtility


DOC_TYPE = 'doc'


class IElasticSearchUtility(ICatalogUtility, IAsyncUtility):
    pass


class IIndexManager(Interface):

    async def get_indexes() -> list:
        '''
        Return a list of active indexes in use for the content
        '''

    async def get_mapping() -> dict:
        '''
        Return a mapping definition
        '''

    async def get_index_settings() -> dict:
        '''
        Returns settings for index
        '''

    async def get_index_name() -> str:
        '''
        Return current active index used for alias
        '''

    async def get_real_index_name() -> str:
        '''
        Return current active index name that alias is pointing to
        '''

    async def get_migration_index_name() -> str:
        '''
        Return name of current active migration index
        '''

    async def start_migration(force : bool = False) -> None:
        '''
        Specify that we're starting a migration,
        get next index name
        '''

    async def finish_migration() -> None:
        '''
        Save migration index as current index
        '''

    async def cancel_migration() -> None:
        '''
        Remove migration index registration
        '''


class IContentIndex(Interface):
    '''
    Content type which provides it's own index to children
    '''


class IIndexActive(Interface):
    '''
    Interface applied to content to mark that it
    has had the elasticsearch index created and
    content is getting added
    '''
