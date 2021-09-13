from guillotina.async_util import IAsyncUtility
from guillotina.catalog.types import BasicParsedQueryInfo
from guillotina.interfaces import ICatalogUtility
from zope.interface import Interface

import typing


SUB_INDEX_SEPERATOR = "__"


class ParsedQueryInfo(BasicParsedQueryInfo):
    query: typing.Dict


class IElasticSearchUtility(ICatalogUtility, IAsyncUtility):
    pass


class IIndexManager(Interface):
    async def get_indexes() -> list:
        """
        Return a list of active indexes in use for the content
        """

    async def get_mapping() -> dict:
        """
        Return a mapping definition
        """

    async def get_index_settings() -> dict:
        """
        Returns settings for index
        """

    async def get_index_name() -> str:
        """
        Return current active index used for alias
        """

    async def get_real_index_name() -> str:
        """
        Return current active index name that alias is pointing to
        """

    async def get_migration_index_name() -> str:
        """
        Return name of current active migration index
        """

    async def start_migration(force: bool = False) -> None:
        """
        Specify that we're starting a migration,
        get next index name
        """

    async def finish_migration() -> None:
        """
        Save migration index as current index
        """

    async def cancel_migration() -> None:
        """
        Remove migration index registration
        """

    async def get_registry():
        """
        Return registry object where index data is stored
        """


class IConnectionFactoryUtility(Interface):
    """
    Be able to customize es connection ob used for a particular
    container/request
    """

    def get(loop=None):
        """
        Get a connection for a request
        """

    async def close(loop=None):
        """
        close all connections
        """
