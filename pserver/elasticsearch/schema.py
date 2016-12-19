from plone.server.catalog.utils import get_index_fields
from plone.server.content import IResourceFactory
from zope.component import getUtilitiesFor


CATALOG_TYPES = {
    'searchabletext': {
        'type': 'text',
        'index': 'analyzed'
    },
    'text': {
        'type': 'text',
        'index': 'not_analyzed'
    },
    'keyword': {
        'type': 'keyword',
        'index': 'not_analyzed'
    },
    'int': {'type': 'integer'},
    'date': {'type': 'date'},
    'boolean': {'type': 'boolean'},
    'binary': {'type': 'binary'},
    'float': {'type': 'float'},
    'nested': {'type': 'nested'},
    'path': {
        "type": "text",
        "analyzer": "path_analyzer"
    }
}


def get_mappings():
    from plone.server import app_settings
    mapping_overrides = app_settings.get('elasticsearch', {}).get('mapping_overrides', {})
    # Mapping calculated from schemas
    global_mappings = {}
    base_type_overrides = mapping_overrides.get('*', {})
    for name, schema in getUtilitiesFor(IResourceFactory):
        # For each type
        mappings = {}
        type_overrides = base_type_overrides.copy()
        type_overrides.update(mapping_overrides.get(name, {}))
        for field_name, catalog_info in get_index_fields(name).items():
            catalog_type = catalog_info.get('type', 'text')
            field_mapping = CATALOG_TYPES[catalog_type]
            if field_name in type_overrides:
                field_mapping = type_overrides[field_name]
            mappings[field_name] = field_mapping
        global_mappings[name] = {
            'properties': mappings
        }
    return global_mappings
