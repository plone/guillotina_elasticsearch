from elasticsearch import AsyncElasticsearch as BaseAsyncElasticsearch


ELASTICSEARCH_COMPATIBILITY_HEADERS = {
    "accept": "application/vnd.elasticsearch+json; compatible-with=8",
    "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
}


def _compatibility_header_value(value):
    if value and "x-ndjson" in value.lower():
        return "application/vnd.elasticsearch+x-ndjson; compatible-with=8"
    return "application/vnd.elasticsearch+json; compatible-with=8"


def apply_compatibility_headers(headers=None):
    headers = (headers or {}).copy()
    header_names = {key.lower(): key for key in headers.keys()}
    for key in ELASTICSEARCH_COMPATIBILITY_HEADERS.keys():
        original_key = header_names.get(key, key)
        headers[original_key] = _compatibility_header_value(headers.get(original_key))
    return headers


def get_connection_settings(settings):
    connection_settings = (settings or {}).copy()
    if (
        "timeout" in connection_settings
        and "request_timeout" not in connection_settings
    ):
        connection_settings["request_timeout"] = connection_settings.pop("timeout")
    else:
        connection_settings.pop("timeout", None)
    connection_settings["headers"] = apply_compatibility_headers(
        connection_settings.get("headers")
    )
    return connection_settings


class AsyncElasticsearch(BaseAsyncElasticsearch):
    async def perform_request(
        self,
        method,
        path,
        *,
        params=None,
        headers=None,
        body=None,
        endpoint_id=None,
        path_parts=None,
    ):
        return await super().perform_request(
            method,
            path,
            params=params,
            headers=apply_compatibility_headers(headers),
            body=body,
            endpoint_id=endpoint_id,
            path_parts=path_parts,
        )
