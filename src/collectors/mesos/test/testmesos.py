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
    STATE = {
        'attributes': {
            'group': 'test'
        }
    }

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
        fixtures = [self.getFixture('slave_metrics'),
                    StringIO(json.dumps(self.STATE)),
                    StringIO("[]")]
        patch_urlopen = patch('urllib2.urlopen',
                              Mock(side_effect=lambda *args: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        published_metrics = {
            'slave_executors_terminated': 397,
            'slave_valid_status_updates': 1471
        }

        self.assertPublishedMany(publish_mock, published_metrics)

    @patch.object(Collector, 'publish')
    def test_task_metrics(self, publish_mock):
        fixtures = [StringIO("{}"),
                    StringIO(json.dumps(self.STATE)),
                    self.getFixture('slave_perf_1'),

                    StringIO("{}"),
                    StringIO(json.dumps(self.STATE)),
                    self.getFixture('slave_perf_2')]
        patch_urlopen = patch('urllib2.urlopen',
                              Mock(side_effect=lambda *args: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        self.collector.collect()
        patch_urlopen.stop()

        expected_metrics = {
            'job_resources_used_user_cpu': 2.7,
            'job_resources_used_sys_cpu': 1,
            'job_resources_used_mem_reserved': 79015936 / (1024 * 1024)
        }
        # reverse it so the calls that get checked are the last calls
        publish_mock.call_args_list.reverse()
        self.assertPublishedMany(publish_mock, expected_metrics, 2)

################################################################################
if __name__ == "__main__":
    unittest.main()
