from pytest_docker_fixtures import images

import os


ES_VERSION = os.environ.get("ES_VERSION", "7.5.1")


images.configure(
    "elasticsearch",
    "docker.elastic.co/elasticsearch/elasticsearch",
    ES_VERSION,
    env={
        "xpack.security.enabled": None,  # unset
        "discovery.type": "single-node",
        "http.host": "0.0.0.0",
        "transport.host": "127.0.0.1",
    },
)


pytest_plugins = [
    "pytest_docker_fixtures",
    "guillotina.tests.fixtures",
    "guillotina_elasticsearch.tests.fixtures",
]
