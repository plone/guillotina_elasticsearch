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


logger = logging.getLogger("guillotina_cms")

MAX_AGGS = 20
SEARCH_DATA_FIELDS = [
    "contributors",
    "creation_date",
    "creators",
    "id",
    "modification_date",
    "parent_uuid",
    "path",
    "tags",
    "title",
    "type_name",
    "uuid",
]


def convert(value):
    # XXX: Check for possible json injection
    return value.split(" ")


def process_compound_field(field, value, operator):
    if not isinstance(value, dict):
        return
    query = {}
    for kk, vv in value.items():
        if operator == "or":
            query[kk + "__should"] = vv
        else:
            query[kk] = vv
    return "must", {"bool": process_query_level(query)}


def process_field(field, value):
    if field.endswith("__or"):
        return process_compound_field(field, value, "or")
    elif field.endswith("__and"):
        field = field[: -len("__and")]
        return process_compound_field(field, value, "and")

    modifier = None

    match_type = "must"
    if field.endswith("__should"):
        match_type = "should"
        field = field[: -len("__should")]
    if field.endswith("__not"):
        modifier = "not"
        field = field[: -len("__not")]
    elif field.endswith("__in"):
        modifier = "in"
        field = field[: -len("__in")]
    elif field.endswith("__eq"):
        modifier = "eq"
        field = field[: -len("__eq")]
    elif field.endswith("__gt"):
        modifier = "gt"
        field = field[: -len("__gt")]
    elif field.endswith("__lt"):
        modifier = "lt"
        field = field[: -len("__lt")]
    elif field.endswith("__gte"):
        modifier = "gte"
        field = field[: -len("__gte")]
    elif field.endswith("__lte"):
        modifier = "lte"
        field = field[: -len("__lte")]
    elif field.endswith("__wildcard"):
        modifier = "wildcard"
        field = field[: -len("__wildcard")]

    index = get_index_definition(field)
    if index is None:
        return
    _type = index["type"]
    if not isinstance(value, list):
        value = [value]
        term_keyword = "term"
    else:
        if len(value) > 1:
            term_keyword = "terms"
        else:
            term_keyword = "term"
    result_list = []
    for value_list in value:
        value_cast = None
        if _type == "int":
            try:
                value_cast = int(value_list)
            except ValueError:
                pass
        elif _type == "date":
            value_cast = parse(value_list).timestamp()

        elif _type == "boolean":
            if value_list in ("true", "True", "yes", True):
                value_cast = "true"
            else:
                value_cast = "false"
        if value_cast:
            result_list.append(value_cast)
        else:
            result_list.append(value_list)
    if len(result_list) == 1:
        value = result_list[0]
    else:
        value = result_list

    if modifier is None:
        # Keyword we expect an exact match
        return match_type, {term_keyword: {field: value}}
    elif modifier == "not":
        # Must not be
        return "must_not", {term_keyword: {field: value}}
    elif modifier == "in" and _type in ("text", "searchabletext"):
        # The value list can be inside the field
        return match_type, {"match": {field: value}}
    elif modifier == "eq":
        # The sentence must appear as is it
        value = " ".join(value)
        return match_type, {"match": {field: value}}
    elif modifier in ("gte", "lte", "gt", "lt"):
        return match_type, {"range": {field: {modifier: value}}}
    elif modifier == "wildcard":
        return match_type, {"wildcard": {field: value}}
    else:
        logger.warn(
            "wrong search type: %s modifier: %s field: %s value: %s"
            % (_type, modifier, field, value)
        )


def process_query_level(params):
    query = {"must": [], "should": [], "minimum_should_match": 1, "must_not": []}
    for field, value in params.items():
        result = process_field(field, value)
        if result is not None:
            match_type, sub_part = result
            query[match_type].append(sub_part)

    if len(query["should"]) == 0:
        del query["should"]
        del query["minimum_should_match"]
    return query


@configure.adapter(
    for_=(IElasticSearchUtility, IResource), provides=ISearchParser, name="default"
)
class Parser(BaseParser):
    def __call__(self, params: typing.Dict) -> ParsedQueryInfo:
        query_info = super().__call__(params)

        metadata = query_info.get("metadata", [])
        if metadata:
            search_data = SEARCH_DATA_FIELDS + metadata
        else:
            search_data = SEARCH_DATA_FIELDS
        query = {
            "stored_fields": search_data,
            "query": {"bool": process_query_level(query_info["params"])},
            "sort": [],
        }
        if query_info["sort_on"]:
            query["sort"].append(
                {query_info["sort_on"]: (query_info["sort_dir"] or "asc").lower()}
            )
        query["sort"].append({"_id": "desc"})
        query["from"] = query_info.get("_from", 0)
        query["size"] = query_info.get("size", 0)
        return typing.cast(ParsedQueryInfo, query)
