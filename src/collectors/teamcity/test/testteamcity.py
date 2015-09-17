#!/usr/bin/python
# coding=utf-8
################################################################################

try:
  from cStringIO import StringIO
  StringIO  # workaround for pyflakes issue #13
except ImportError:
  from StringIO import StringIO

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import Mock
from mock import patch

from diamond.collector import Collector

from teamcity import TeamCityCollector

################################################################################

class TestTeamCityCollector(CollectorTestCase):

    def setUp(self):
        config = get_collector_config('TeamCityCollector', {
          'hosts': 'localhost:8111',
        })

        self.collector = TeamCityCollector(config, None)

    @patch.object(Collector, 'publish')
    def test_metrics(self, publish_mock):
        fixtures = [
            StringIO('1.0.0'),
            StringIO('{"count": 3}')
        ]

        patch_urlopen = patch('urllib2.urlopen', Mock(
            side_effect=lambda *args: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        published_metrics = {
            'server.available': 1,
            'server.connected_agents': 3
        }

        self.assertPublishedMany(publish_mock, published_metrics)

    @patch.object(Collector, 'publish')
    def test_metrics_no_server(self, publish_mock):
        def boom(*args):
            raise Exception('boom')

        patch_urlopen = patch('urllib2.urlopen', Mock(
            side_effect=boom))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        published_metrics = {
            'server.available': 0,
            'server.connected_agents': 0
        }

        self.assertPublishedMany(publish_mock, published_metrics)

################################################################################
if __name__ == "__main__":
    unittest.main()
