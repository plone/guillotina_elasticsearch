from zope.interface import Attribute
from zope.interface import implementer
from zope.interface import Interface


class ISearchDoneEvent(Interface):
    users = Attribute('List of users doing the call')

    query = Attribute('The query')

    total = Attribute('The total amount of results for this query')

    request = Attribute('Request responsible')


@implementer(ISearchDoneEvent)
class SearchDoneEvent(object):
    """An object is going to be assigned to an attribute on another object."""

    def __init__(self, users, query, total, request):
        self.users = users
        self.query = query
        self.total = total
        self.request = request
