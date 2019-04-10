from guillotina import app_settings
from guillotina_elasticsearch.interfaces import IElasticSearchUtility
from guillotina.component import get_utility
from guillotina.tests.utils import wrap_request

import os
import pytest


@pytest.mark.skipif(
    os.environ.get('ES_VERSION', '6') != 'OPEN_DISTRO',
    reason='Only for opendistro')
async def test_opendistro_index_with_security_user(
        es_requester, guillotina, dummy_request):
    async with wrap_request(dummy_request):
        utility = get_utility(IElasticSearchUtility)
        conn = utility.get_connection()
        await conn.indices.create('new_container', {
            "settings": {
                "number_of_shards": 1
            },
            "mappings": {
                "_doc": {
                    "properties": {
                        "field1": {
                            "type": "text"
                        }
                    }
                }
            }
        })
        conn_es = await conn.transport.get_connection()
        async with conn_es.session.put(
            os.path.join(
                str(conn_es.base_url),
                '_opendistro/_security/api/internalusers/new_container'),
            json={
                "password": "new_container",
                "roles": ["own_index"]
            }
        ) as resp:
            assert resp.status == 201

        app_settings['elasticsearch']['new_container_settings'] = {  # noqa
            "http_auth": "new_container:new_container"
        }
        dummy_request._es_conn = None
        dummy_request._container_id = 'new_container'
        conn2 = utility.get_connection()
        conn_es2 = await conn2.transport.get_connection()
        assert conn != conn2
        assert conn_es2.http_auth.login == 'new_container'
