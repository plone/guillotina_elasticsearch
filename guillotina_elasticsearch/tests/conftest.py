from pytest_docker_fixtures import images


image_version_8 = "8.12.0-debian-11-r2"
image_version_7 = "7.17.16-debian-11-r3"  # noqa

images.configure(
    name="elasticsearch",
    full=f"bitnami/elasticsearch:{image_version_8}",
    max_wait_s=90,
    env={"ELASTICSEARCH_ENABLE_SECURITY": "false", "ELASTICSEARCH_HEAP_SIZE": "1g"},
)


pytest_plugins = [
    "pytest_docker_fixtures",
    "guillotina.tests.fixtures",
    "guillotina_elasticsearch.tests.fixtures",
]
