from pytest_docker_fixtures import images
import os

if os.environ.get('ES_VERSION', '6') == '6':
    images.configure(
        'elasticsearch',
        'docker.elastic.co/elasticsearch/elasticsearch-oss', '6.7.0',
        env={
            "xpack.security.enabled": None  # unset
        })
elif os.environ.get('ES_VERSION', '6') == 'OPEN_DISTRO':
    images.configure(
        'elasticsearch',
        'amazon/opendistro-for-elasticsearch', '0.7.1',
        env={
            "xpack.security.enabled": None,
            "discovery.type": "single-node",
            "http.host": "0.0.0.0",
            "transport.host": "127.0.0.1"
        })


pytest_plugins = [
    'pytest_docker_fixtures',
    'guillotina.tests.fixtures',
    'guillotina_elasticsearch.tests.fixtures',
]
