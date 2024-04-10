from guillotina import app_settings
from guillotina.component import get_utilities_for
from guillotina.content import get_all_possible_schemas_for_type
from guillotina.content import IResourceFactory
from typing import Any
from typing import Dict

import guillotina.directives


CATALOG_TYPES: Dict[str, Any] = {
    "searchabletext": {"type": "text", "index": True},
    "text": {"type": "text", "index": True},
    "keyword": {"type": "keyword", "index": True},
    "textkeyword": {
        "type": "text",
        "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
    },
    "int": {"type": "integer"},
    "date": {"type": "date"},
    "boolean": {"type": "boolean"},
    "binary": {"type": "binary"},
    "long": {"type": "long"},
    "float": {"type": "float"},
    "nested": {"type": "nested"},
    "object": {"type": "object"},
    "completion": {"type": "completion"},
    "path": {"type": "text", "analyzer": "path_analyzer"},
}


def merged_tagged_value_dict(iface, name):
    """
    Alternative implementation the keeps info on schema it came from
    """
    tv = {}
    for iface in reversed(iface.__iro__):
        value = iface.queryTaggedValue(name, None)
        if value is not None:
            for k, v in value.items():
                v["__schema__"] = iface
            tv.update(value)
    return tv


def _addon_index(ob):
    # find the index of the addon the ob is part of
    idx = -1
    for i, addon in enumerate(app_settings["applications"]):
        if ob.__module__.startswith(addon + "."):
            idx = i
    return idx


def get_mappings(schemas=None, schema_info=False):
    if schemas is None:
        schemas = []
        for name, _ in get_utilities_for(IResourceFactory):
            # For each type
            for schema in get_all_possible_schemas_for_type(name):
                schemas.append(schema)
        schemas = set(schemas)

    mappings = {}
    schema_field_mappings = {}
    for schema in schemas:
        index_fields = merged_tagged_value_dict(schema, guillotina.directives.index.key)
        for field_name, catalog_info in index_fields.items():
            index_name = catalog_info.get("index_name", field_name)
            catalog_type = catalog_info.get("type", "text")
            field_mapping = catalog_info.get("field_mapping", None)
            if field_mapping is None:
                field_mapping = CATALOG_TYPES[catalog_type].copy()
            if "store" in catalog_info:
                field_mapping["store"] = catalog_info["store"]
            if "analyzer" in catalog_info:
                field_mapping["analyzer"] = catalog_info["analyzer"]
            if "normalizer" in catalog_info:
                field_mapping["normalizer"] = catalog_info["normalizer"]
            if "multifields" in catalog_info:
                field_mapping["fields"] = catalog_info["multifields"]
            if "search_analyzer" in catalog_info:
                field_mapping["search_analyzer"] = catalog_info["search_analyzer"]
            if schema_info:
                if "_schemas" not in field_mapping:
                    field_mapping["_schemas"] = []
                if schema.__identifier__ not in field_mapping["_schemas"]:
                    field_mapping["_schemas"].append(schema.__identifier__)

            if index_name in mappings and mappings[index_name] != field_mapping:
                existing_addon_idx = _addon_index(schema_field_mappings[index_name])
                field_addon_idx = _addon_index(catalog_info["__schema__"])
                if existing_addon_idx > field_addon_idx:
                    # we're keeping current value
                    continue
                elif existing_addon_idx == field_addon_idx:
                    # we are customizing same field mapping in same addon!
                    # this should not be allowed
                    raise Exception(
                        f"""Unresolvable index mapping conflict: {index_name}
Registered schema: {schema_field_mappings[index_name].__identifier__}
Registered mapping: {mappings[index_name]}
Conflicted schema: {catalog_info['__schema__'].__identifier__}
Registered mapping: {field_mapping}
"""
                    )

            schema_field_mappings[index_name] = catalog_info["__schema__"]
            mappings[index_name] = field_mapping
    return {
        "properties": mappings,
        "dynamic": app_settings.get("elasticsearch", {}).get("dynamic_mapping", False),
    }
