from pytest_docker_fixtures import images
import os

if os.environ.get('ES_VERSION', '6') == '6':
    images.configure(
        'elasticsearch',
        'docker.elastic.co/elasticsearch/elasticsearch-oss', '6.2.4')


pytest_plugins = [
    'pytest_docker_fixtures',
    'guillotina.tests.fixtures',
    'guillotina_elasticsearch.tests.fixtures',
]
