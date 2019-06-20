# https://docs.google.com/document/d/1xooubzJKBnUzVlsa2f0GSwvuE1oir9hUdWUf1SU9S6E/edit
from dateutil.parser import parse
from guillotina import configure
from guillotina.catalog.parser import BaseParser
from guillotina.catalog.utils import get_index_definition
from guillotina.interfaces import IResource
from guillotina.interfaces import ISearchParser
from guillotina_elasticsearch.interfaces import IElasticSearchUtility
from guillotina_elasticsearch.interfaces import ParsedQueryInfo

import logging
import typing


logger = logging.getLogger('guillotina_cms')

MAX_AGGS = 20
SEARCH_DATA_FIELDS = [
    'content_layout',
    'contributors',
    'creation_date',
    'creators',
    'hidden_navigation',
    'id',
    'language',
    'modification_date',
    'parent_uuid',
    'path',
    'review_state',
    'tags',
    'title',
    'type_name',
    'uuid'
]


def convert(value):
    # XXX: Check for possible json injection
    return value.split(' ')


def process_field(field, value, query):

    modifier = None

    match_type = 'must'
    if field.endswith('__should'):
        match_type = 'should'
        field = field.rstrip('__should')

    if field.endswith('__not'):
        modifier = 'not'
        field = field.rstrip('__not')
    elif field.endswith('__in'):
        modifier = 'in'
        field = field.rstrip('__in')
    elif field.endswith('__eq'):
        modifier = 'eq'
        field = field.rstrip('__eq')
    elif field.endswith('__gt'):
        modifier = 'gt'
        field = field.rstrip('__gt')
    elif field.endswith('__lt'):
        modifier = 'lt'
        field = field.rstrip('__lt')
    elif field.endswith('__gte'):
        modifier = 'gte'
        field = field.rstrip('__gte')
    elif field.endswith('__lte'):
        modifier = 'lte'
        field = field.rstrip('__lte')
    elif field.endswith('__wildcard'):
        modifier = 'wildcard'
        field = field.rstrip('__wildcard')

    index = get_index_definition(field)
    if index is None:
        return

    if len(value) > 1:
        term_keyword = 'terms'
    else:
        term_keyword = 'term'
        value = value[0]

    _type = index['type']
    if _type == 'int':
        try:
            value = int(value)
        except ValueError:
            pass
    elif _type == 'date':
        value = parse(value).timestamp()

    elif _type == 'boolean':
        if value in ('true', 'True', 'yes'):
            value = True
        else:
            value = False

    if modifier is None:
        # Keyword we expect an exact match
        query['query']['bool'][match_type].append(
            {
                term_keyword: {
                    field: value
                }
            })
    elif modifier == 'not':
        # Must not be
        query['query']['bool']['must_not'].append(
            {
                term_keyword: {
                    field: value
                }
            })
    elif modifier == 'in' and _type in ('text', 'searchabletext'):
        # The value list can be inside the field
        query['query']['bool'][match_type].append(
            {
                'match': {
                    field: value
                }
            })
    elif modifier == 'eq':
        # The sentence must appear as is it
        value = ' '.join(value)
        query['query']['bool'][match_type].append(
            {
                'match': {
                    field: value
                }
            })
    elif modifier in ('gte', 'lte', 'gt', 'lt'):
        query['query']['bool'][match_type].append(
            {
                'range': {field: {modifier: value}}})
    elif modifier == 'wildcard':
        query['query']['bool'][match_type].append(
            {
                'wildcard': {
                    field: value
                }
            })
    else:
        logger.warn(
            'wrong search type: %s modifier: %s field: %s value: %s' %
            (_type, modifier, field, value))


@configure.adapter(
    for_=(IElasticSearchUtility, IResource),
    provides=ISearchParser,
    name='default')
class Parser(BaseParser):

    def __init__(self, request, context):
        self.request = request
        self.context = context

    def __call__(self, params: typing.Dict) -> ParsedQueryInfo:
        query_info = super().__call__(params)

        query = {
            'stored_fields': SEARCH_DATA_FIELDS,
            'query': {
                'bool': {
                    'must': [],
                    'should': [],
                    "minimum_should_match": 1,
                    'must_not': []
                }
            },
            'sort': []
        }

        for field, value in query_info['params'].items():
            process_field(field, value, query)

        return typing.cast(ParsedQueryInfo, dict(
            query_info,
            query=query
        ))

        if query_info['sort_on']:
            query['sort'].append({
                query_info['sort_on']: (
                    query_info['sort_dir'] or 'asc').lower()
            })
        query['sort'].append({'_id': 'desc'})

        if len(query['query']['bool']['should']) == 0:
            del query['query']['bool']['should']
            del query['query']['bool']['minimum_should_match']

        return typing.cast(ParsedQueryInfo, dict(
            query_info,
            query=query
        ))
