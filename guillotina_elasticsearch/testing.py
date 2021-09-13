from elasticsearch import AsyncElasticsearch
from guillotina import app_settings
from guillotina import configure
from guillotina import task_vars
from guillotina.exceptions import RequestNotFound
from guillotina_elasticsearch.interfaces import IConnectionFactoryUtility
from guillotina_elasticsearch.utility import DefaultConnnectionFactoryUtility

import asyncio


@configure.utility(provides=IConnectionFactoryUtility)
class CustomConnSettingsUtility(DefaultConnnectionFactoryUtility):
    """
    test to demonstrate using different settings from configuration
    """

    def __init__(self):
        super().__init__()
        self._special_conn = None

    def get(self, loop=None):
        container = None
        try:
            container = task_vars.container.get()
        except RequestNotFound:
            return super().get(loop)

        settings = app_settings.get("elasticsearch", {}).get("connection_settings")
        if (
            container is None
            or container.id != "new_container"
            or "new_container_settings" not in app_settings["elasticsearch"]
        ):
            return super().get(loop)
        else:
            if self._special_conn is None:
                settings = settings.copy()
                settings.update(app_settings["elasticsearch"]["new_container_settings"])
                self._special_conn = AsyncElasticsearch(loop=loop, **settings)
            return self._special_conn

    async def close(self, loop=None):
        await super().close(loop)
        if self._special_conn is not None:
            if loop is not None:
                asyncio.run_coroutine_threadsafe(self._conn.close(), loop)
            else:
                await self._special_conn.close()
