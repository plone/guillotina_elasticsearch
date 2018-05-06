from aioelasticsearch import exceptions

import logging


logger = logging.getLogger('guillotina_elasticsearch')

class NoopResponse:
    def write(self, *args, **kwargs):
        pass


noop_response = NoopResponse()


async def safe_es_call(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except exceptions.ConnectionError:
        logger.warning('elasticsearch not installed', exc_info=True)
    except (exceptions.RequestError, exceptions.NotFoundError,
            RuntimeError):
        pass
    except exceptions.TransportError as e:
        logger.warning('Transport Error', exc_info=e)
