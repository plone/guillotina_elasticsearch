from guillotina.directives import MetadataDictDirective


class index(MetadataDictDirective):  # noqa: N801
    key = 'guillotina_elasticsearch.directives.index'

    def factory(self, **kw):
        return kw
