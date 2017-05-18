from guillotina.catalog.utils import get_index_fields
from guillotina.component import getUtilitiesFor
from guillotina.content import IResourceFactory


CATALOG_TYPES = {
    'searchabletext': {
        'type': 'text',
        'index': True
    },
    'text': {
        'type': 'text',
        'index': True
    },
    'keyword': {
        'type': 'keyword',
        'index': True
    },
    'textkeyword': {
        'type': 'text',
        'fields': {
            'keyword':  {
                'type': 'keyword',
                'ignore_above': 256
            }
        }
    },
    'int': {'type': 'integer'},
    'date': {'type': 'date'},
    'boolean': {'type': 'boolean'},
    'binary': {'type': 'binary'},
    'long': {'type': 'long'},
    'float': {'type': 'float'},
    'nested': {'type': 'nested'},
    'object': {'type': 'object'},
    'completion': {'type': 'completion'},
    'path': {
        'type': 'text',
        'analyzer': 'path_analyzer'
    }
}


def get_mappings():
    from guillotina import app_settings
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
            field_mapping = catalog_info.get('field_mapping', None)
            if field_mapping is None:
                field_mapping = CATALOG_TYPES[catalog_type].copy()
            if field_name in type_overrides:
                field_mapping = type_overrides[field_name]
            mappings[field_name] = field_mapping
        global_mappings[name] = {
            'properties': mappings
        }
    return global_mappings
