from plone.server.interfaces import CATALOG_KEY
from plone.server.interfaces import INDEX_KEY
from plone.server.directives import mergedTaggedValueDict
from zope.component import getUtilitiesFor
from plone.server.content import IResourceFactory
from plone.server.content import iterSchemataForType
from zope.schema import getFields


CATALOG_TYPES = {
    'text': {'type': 'text'},
    'keyword': {'type': 'keyword'},
    'int': {'type': 'integer'},
    'date': {'type': 'date'},
    'boolean': {'type': 'boolean'},
    'binary': {'type': 'binary'},
    'float': {'type': 'float'}
}

INDEX_TYPES = {
    'analyzed': {'index': 'analyzed'},
    'non_analyzed': {'index': 'not_analyzed'}
}


def get_mappings():
    # Mapping calculated from schemas
    global_mappings = {}
    for name, schema in getUtilitiesFor(IResourceFactory):
        # For each type
        mappings = {}
        for schema in iterSchemataForType(name):
            # create mapping for content type
            catalog = mergedTaggedValueDict(schema, CATALOG_KEY)
            index = mergedTaggedValueDict(schema, INDEX_KEY)
            for field_name, field in getFields(schema).items():
                kind_index = index.get(field_name, False)
                kind_catalog = catalog.get(field_name, False)
                field_mapping = {}
                if kind_catalog:
                    if kind_catalog == 'object':
                        # Especial case that is an object
                        # TODO
                        pass
                    field_mapping.update(CATALOG_TYPES[kind_catalog])
                    if kind_index:
                        field_mapping.update(INDEX_TYPES[kind_index])

                    field_name = schema.getName() + '-' + field_name
                    mappings[field_name] = field_mapping
        mappings['accessRoles'] = {
            'type': 'keyword',
            'index': 'not_analyzed'
        }
        mappings['accessUsers'] = {
            'type': 'keyword',
            'index': 'not_analyzed'
        }
        mappings['path'] = {
            'type': 'text',
            'analyzer': 'path_analyzer'
        }
        mappings['uuid'] = {
            'type': 'keyword',
            'index': 'not_analyzed'
        }
        global_mappings[name] = {
            'properties': mappings
        }
    return global_mappings
