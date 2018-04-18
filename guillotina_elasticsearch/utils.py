from aioes import exception

import logging


logger = logging.getLogger('guillotina_elasticsearch')

class NoopResponse:
    def write(self, *args, **kwargs):
        pass


noop_response = NoopResponse()


async def safe_es_call(func, *args, **kwargs):
    try:
        return await func(*args, **kwargs)
    except exception.ConnectionError:
        logger.warning('elasticsearch not installed', exc_info=True)
    except (exception.RequestError, exception.NotFoundError,
            RuntimeError):
        pass
    except exception.TransportError as e:
        logger.warning('Transport Error', exc_info=e)
