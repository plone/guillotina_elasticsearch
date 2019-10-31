from aiohttp.web_exceptions import HTTPException


class QueryErrorException(HTTPException):
    status_code = 488


class ElasticsearchConflictException(Exception):
    def __init__(self, conflicts, resp):
        self.conflicts = conflicts
        self.response = resp
        super().__init__(f"{self.conflicts} on ES request")
