from guillotina.commands import Command
from guillotina.content import get_all_possible_schemas_for_type
from guillotina.utils import resolve_dotted_name
from guillotina_elasticsearch.schema import get_mappings
from pprint import pprint

import operator


class FieldsCommand(Command):
    description = "Report on configured fields"
    type_counts = {}
    schema_counts = {}
    total = stored = 0

    selected_schemas = None

    def get_parser(self):
        parser = super().get_parser()
        parser.add_argument("--summary", action="store_true")
        parser.add_argument("--schema", action="append")
        parser.add_argument("--type", action="append")
        return parser

    def _count_field(self, field, schemas=None):
        if "properties" in field:
            for sub_field in field["properties"].values():
                self._count_field(sub_field, field["_schemas"])
            return

        if schemas is None:
            schemas = field["_schemas"]

        self.total += 1
        if field.get("store"):
            self.stored += 1
        if field["type"] not in self.type_counts:
            self.type_counts[field["type"]] = 0
        for schema_name in schemas:
            if schema_name not in self.schema_counts:
                self.schema_counts[schema_name] = 0
            self.schema_counts[schema_name] += 1
        self.type_counts[field["type"]] += 1

    def summary(self):
        for field in get_mappings(self.selected_schemas, schema_info=True)[
            "properties"
        ].values():  # noqa
            self._count_field(field)

        pprint(
            {
                "total": self.total,
                "stored": self.stored,
                "type_counts": sorted(
                    self.type_counts.items(), key=operator.itemgetter(1), reverse=True
                ),
                "schema_counts": sorted(
                    self.schema_counts.items(), key=operator.itemgetter(1), reverse=True
                ),
            }
        )

    async def run(self, arguments, settings, app):
        if arguments.schema:
            self.selected_schemas = [resolve_dotted_name(s) for s in arguments.schema]
        if arguments.type:
            if self.selected_schemas is None:
                self.selected_schemas = []
            for type_name in arguments.type:
                for schema in get_all_possible_schemas_for_type(type_name):
                    self.selected_schemas.append(schema)
        if self.arguments.summary:
            self.summary()
        else:
            fields = get_mappings(self.selected_schemas, schema_info=True)["properties"]
            pprint(fields)
