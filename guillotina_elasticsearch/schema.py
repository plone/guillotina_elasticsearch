from guillotina.component import get_utilities_for
from guillotina.content import get_all_possible_schemas_for_type
from guillotina.content import IResourceFactory

import guillotina.directives


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
            'keyword': {
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


def get_mappings(schemas=None):

    if schemas is None:
        schemas = []
        for name, _ in get_utilities_for(IResourceFactory):
            # For each type
            for schema in get_all_possible_schemas_for_type(name):
                schemas.append(schema)
        schemas = set(schemas)

    mappings = {}
    for schema in schemas:
        index_fields = guillotina.directives.merged_tagged_value_dict(
            schema, guillotina.directives.index.key)
        for field_name, catalog_info in index_fields.items():
            index_name = catalog_info.get('index_name', field_name)
            catalog_type = catalog_info.get('type', 'text')
            field_mapping = catalog_info.get('field_mapping', None)
            if field_mapping is None:
                field_mapping = CATALOG_TYPES[catalog_type].copy()
            if 'store' in catalog_info:
                field_mapping['store'] = catalog_info['store']
            mappings[index_name] = field_mapping

    return {
        'properties': mappings,
        'dynamic': False,
        '_all': {
            'enabled': False
        }
    }
