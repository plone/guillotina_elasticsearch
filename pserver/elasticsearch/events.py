from zope.interface import Attribute
from zope.interface import implementer
from zope.interface import Interface


class ISearchDoneEvent(Interface):
    query = Attribute('The query')

    total = Attribute('The total amount of results for this query')

    request = Attribute('Request responsible')

    time = Attribute('Time to process')


@implementer(ISearchDoneEvent)
class SearchDoneEvent(object):
    """An object is going to be assigned to an attribute on another object."""

    def __init__(self, query, total, request, time):
        self.query = query
        self.total = total
        self.request = request
        self.time = time
