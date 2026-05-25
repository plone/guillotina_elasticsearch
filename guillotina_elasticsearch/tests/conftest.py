from pytest_docker_fixtures import images

import os


image_version_9 = "9.4.0"
image_version_8 = "8.12.0"  # noqa
image_version_7 = "7.17.16"  # noqa
# Kept selectable for older release-line compatibility checks.
image_version_6 = "6.8.23"  # noqa

es_major_version = os.environ.get("ES_TEST_VERSION", "9")
image_versions = {
    "6": image_version_6,
    "7": image_version_7,
    "8": image_version_8,
    "9": image_version_9,
}
try:
    image_version = image_versions[es_major_version]
except KeyError as exc:
    versions = ", ".join(sorted(image_versions))
    raise RuntimeError(
        f"Unsupported ES_TEST_VERSION={es_major_version!r}. " f"Use one of: {versions}."
    ) from exc

images.configure(
    name="elasticsearch",
    full=f"docker.elastic.co/elasticsearch/elasticsearch:{image_version}",
    max_wait_s=180,
    env={
        "discovery.type": "single-node",
        "xpack.security.enabled": "false",
        "cluster.routing.allocation.disk.threshold_enabled": "false",
        "ES_JAVA_OPTS": "-Xms1g -Xmx1g",
    },
)


pytest_plugins = [
    "pytest_docker_fixtures",
    "guillotina.tests.fixtures",
    "guillotina_elasticsearch.tests.fixtures",
]
