from aiohttp.web_exceptions import HTTPException


class QueryErrorException(HTTPException):
    status_code = 488
