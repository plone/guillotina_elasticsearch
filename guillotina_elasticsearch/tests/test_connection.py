from elasticsearch import AsyncElasticsearch as BaseAsyncElasticsearch
from guillotina_elasticsearch.connection import apply_compatibility_headers
from guillotina_elasticsearch.connection import AsyncElasticsearch
from guillotina_elasticsearch.connection import get_connection_settings


def test_apply_compatibility_headers_defaults():
    assert apply_compatibility_headers() == {
        "accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
    }


def test_apply_compatibility_headers_keeps_header_case():
    headers = apply_compatibility_headers(
        {
            "Accept": "application/json",
            "Content-Type": "application/x-ndjson",
            "x-opaque-id": "request-1",
        }
    )

    assert headers == {
        "Accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "Content-Type": "application/vnd.elasticsearch+x-ndjson; compatible-with=8",
        "x-opaque-id": "request-1",
    }


def test_get_connection_settings_does_not_mutate_input():
    settings = {
        "hosts": ["http://localhost:9200"],
        "timeout": 30,
        "headers": {"x-opaque-id": "request-1"},
    }

    connection_settings = get_connection_settings(settings)

    assert settings == {
        "hosts": ["http://localhost:9200"],
        "timeout": 30,
        "headers": {"x-opaque-id": "request-1"},
    }
    assert connection_settings["request_timeout"] == 30
    assert "timeout" not in connection_settings
    assert connection_settings["headers"] == {
        "accept": "application/vnd.elasticsearch+json; compatible-with=8",
        "content-type": "application/vnd.elasticsearch+json; compatible-with=8",
        "x-opaque-id": "request-1",
    }


def test_get_connection_settings_prefers_request_timeout():
    settings = {
        "hosts": ["http://localhost:9200"],
        "timeout": 30,
        "request_timeout": 90,
    }

    connection_settings = get_connection_settings(settings)

    assert connection_settings["request_timeout"] == 90
    assert "timeout" not in connection_settings


async def test_perform_request_applies_compatibility_headers(monkeypatch):
    calls = []

    async def perform_request(self, method, path, **kwargs):
        calls.append((method, path, kwargs))
        return {"ok": True}

    monkeypatch.setattr(BaseAsyncElasticsearch, "perform_request", perform_request)

    client = AsyncElasticsearch("http://localhost:9200")
    try:
        response = await client.perform_request(
            "POST",
            "/_bulk",
            headers={
                "accept": "application/json",
                "content-type": "application/x-ndjson",
            },
            body=[],
            endpoint_id="bulk",
        )
    finally:
        await client.close()

    assert response == {"ok": True}
    assert calls == [
        (
            "POST",
            "/_bulk",
            {
                "params": None,
                "headers": {
                    "accept": "application/vnd.elasticsearch+json; compatible-with=8",
                    "content-type": (
                        "application/vnd.elasticsearch+x-ndjson; compatible-with=8"
                    ),
                },
                "body": [],
                "endpoint_id": "bulk",
                "path_parts": None,
            },
        )
    ]
