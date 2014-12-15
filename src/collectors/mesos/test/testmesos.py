#!/usr/bin/python
# coding=utf-8
################################################################################

try:
    from cStringIO import StringIO
    StringIO  # workaround for pyflakes issue #13
except ImportError:
    from StringIO import StringIO

import json

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import Mock
from mock import patch

from diamond.collector import Collector

from mesos import MesosCollector

################################################################################


class TestMesosCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('MesosCollector', {
            'hosts': 'localhost:8081',
        })

        self.collector = MesosCollector(config, None)

    def test_import(self):
        self.assertTrue(MesosCollector)

    @patch.object(Collector, 'publish')
    def test_master_metrics(self, publish_mock):
        mm = self.getFixture('master_metrics')
        patch_urlopen = patch('urllib2.urlopen', Mock(return_value=mm))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        published_metrics = {
            'master_mem_used': 297300,
            'master_slave_registrations': 11,
            'system_mem_free_bytes': 1334034432
        }

        self.assertPublishedMany(publish_mock, published_metrics)

    @patch.object(Collector, 'publish')
    def test_no_publish_if_inactive(self, publish_mock):
        metrics = {
            'master_elected': 0,
            'master_mem_used': 297300,
            'master_slave_registrations': 11,
            'system_mem_free_bytes': 1334034432
        }

        mm = StringIO(json.dumps(metrics))
        patch_urlopen = patch('urllib2.urlopen', Mock(return_value=mm))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        self.assertUnpublishedMany(publish_mock, metrics)

    @patch.object(Collector, 'publish')
    def test_slave_metrics(self, publish_mock):
        mm = self.getFixture('slave_metrics')
        patch_urlopen = patch('urllib2.urlopen', Mock(return_value=mm))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        published_metrics = {
            'slave_executors_terminated': 397,
            'slave_valid_status_updates': 1471
        }

        self.assertPublishedMany(publish_mock, published_metrics)
################################################################################
if __name__ == "__main__":
    unittest.main()
