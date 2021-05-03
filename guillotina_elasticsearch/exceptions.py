from guillotina.response import HTTPError


class QueryErrorException(HTTPError):
    status_code = 488


class ElasticsearchConflictException(Exception):
    def __init__(self, conflicts, resp):
        self.conflicts = conflicts
        self.response = resp
        super().__init__(f"{self.conflicts} on ES request")
