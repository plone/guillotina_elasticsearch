# -*- coding: utf-8 -*-

from guillotina.testing import PloneBaseLayer
from guillotina.testing import TESTING_SETTINGS

import requests
import unittest


TESTING_SETTINGS['applications'] = ['guillotina_elasticsearch']

TESTING_SETTINGS['elasticsearch'] = {
    "bulk_size": 50,
    "index_name_prefix": "guillotina-",
    "connection_settings": {
        "endpoints": ["localhost:9200"],
        "sniffer_timeout": 0.5
    },
    "index": {},
    "mapping_overrides": {
        "*": {
        }
    }
}


class ElasticSearchLayer(PloneBaseLayer):

    def _get_site(self):
        """
        sometimes the site does not get updated data from zodb
        this seems to make it
        """
        return self.layer.new_root()['plone']

    @classmethod
    def setUp(cls):
        pass

    @classmethod
    def testSetUp(cls):
        pass

    @classmethod
    def testTearDown(cls):
        requests.delete('http://localhost:9200/guillotina-guillotina')

    @classmethod
    def tearDown(cls):
        pass


class ElasticSearchTestCase(unittest.TestCase):
    ''' Adding the OAuth utility '''
    layer = ElasticSearchLayer
