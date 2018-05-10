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


class IIndexProgress(Interface):
    request = Attribute('Request responsible')
    context = Attribute("Context where the Index process has started")
    processed = Attribute('Docs finished indexing')
    total = Attribute('Amount of docs to index')
    completed = Attribute("Flag if IndexAction is completed")


@implementer(IIndexProgress)
class IndexProgress(object):

    def __init__(self, request, context, processed, total, completed=None):
        self.request = request
        self.context = context
        self.processed = processed
        self.total = total
        self.completed = completed
