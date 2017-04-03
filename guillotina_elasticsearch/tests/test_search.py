from guillotina_elasticsearch.testing import ElasticSearchTestCase

import json
import time


class FunctionalTestServer(ElasticSearchTestCase):
    """Functional testing of the API REST."""

    def test_search(self):
        resp = self.layer.requester(
            'POST',
            '/plone/plone/',
            data=json.dumps({
                '@type': 'Example',
                'title': 'Item1',
                'id': 'item1',
                'categories': [{
                    'label': 'term1',
                    'number': 1.0
                }, {
                    'label': 'term2',
                    'number': 2.0
                }]
            })
        )
        self.assertTrue(resp.status_code == 201)
        time.sleep(1)
        resp = self.layer.requester(
            'POST',
            '/plone/plone/@search',
            data=json.dumps({})
        )
        json_data = json.loads(resp.text)
        self.assertTrue(json_data['items_count'], 1)
