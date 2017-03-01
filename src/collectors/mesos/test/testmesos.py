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
        },
        'frameworks': [{
            'executors': [{
                'id': 'thermos-1420503024922-docker-test-devel-hello_docker-0-46e0aa39-8691-414a-992c-919ea2d1003d',
                'directory': '/tmp',
                'resources': {
                    'cpus': 10,
                    'disk': 16
                },
                'tasks': [{
                    'executor_id': u'thermos-1420503024922-docker-test-devel-hello_docker-0-46e0aa39-8691-414a-992c-919ea2d1003d'
                }]
            }]
        }],
        'version': '0.22.1'
    }

    def get_state(self, major, minor, point):
        state = self.STATE.copy()
        state['version'] = '%s.%s.%s' % (major, minor, point)
        return state

    def setUp(self):
        config = get_collector_config('MesosCollector', {
            'hosts': 'localhost:8081',
            'collect_disk_usage': False
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
            'master/elected': 0,
            'master_mem_used': 297300,
            'master_slave_registrations': 11,
            'system_mem_free_bytes': 1334034432
        }

        patch_urlopen = patch('urllib2.urlopen',
            Mock(side_effect=lambda *args: StringIO(json.dumps(metrics))))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        self.assertUnpublishedMany(publish_mock, metrics)

    @patch.object(Collector, 'publish')
    def test_slave_metrics(self, publish_mock):
        fixtures = [self.getFixture('slave_metrics'),
                    StringIO(json.dumps(self.STATE)),
                    StringIO("[]")]
        patch_urlopen = patch(
            'urllib2.urlopen',
            Mock(side_effect=lambda *args, **kwargs: fixtures.pop(0)))

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
                    StringIO(json.dumps(self.get_state(0, 22, 1))),
                    self.getFixture('slave_perf_1'),

                    StringIO("{}"),
                    StringIO(json.dumps(self.get_state(0, 22, 1))),
                    self.getFixture('slave_perf_2')]
        patch_urlopen = patch(
            'urllib2.urlopen',
            Mock(side_effect=lambda *args, **kwargs: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        self.collector.collect()
        patch_urlopen.stop()

        expected_metrics = {
            'job_resources_used_user_cpu': 2.7,
            'job_resources_used_sys_cpu': 1,
            'job_resources_used_cpu': 3.7,
            'job_resources_used_mem_total': 79015936 / (1024 * 1024)
        }
        # reverse it so the calls that get checked are the last calls
        publish_mock.call_args_list.reverse()

        # Make sure the sources are right.
        for arg in publish_mock.call_args_list:
            self.assertEqual(arg[1]['source'],
                             'test.docker.devel.hello_docker.0')

        # Make sure the values are correct.
        self.assertPublishedMany(publish_mock, expected_metrics, 2)

        unexpected_metrics = {
            'job_resources_used_mem_file': 0,
            'job_resources_used_mem_resident': 0
        }

        self.assertUnpublishedMany(publish_mock, unexpected_metrics)

    @patch.object(Collector, 'publish')
    def test_task_memory_metrics(self, publish_mock):
        fixtures = [StringIO("{}"),
                    StringIO(json.dumps(self.get_state(0, 22, 1))),
                    self.getFixture('slave_perf_3')]

        patch_urlopen = patch(
            'urllib2.urlopen',
            Mock(side_effect=lambda *args, **kwargs: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        expected_metrics = {
            'job_resources_used_mem_total': 79015936 / (1024 * 1024),
            'job_resources_used_mem_file': 22227836 / (1024 * 1024),
            'job_resources_used_mem_resident': 56788100 / (1024 * 1024)
        }

        self.assertPublishedMany(publish_mock, expected_metrics)

    @patch.object(Collector, 'publish')
    def test_task_indirect_disk_metrics(self, publish_mock):
        fixtures = [StringIO("{}"),
                    StringIO(json.dumps(self.get_state(0, 22, 1))),
                    self.getFixture('slave_perf_3')]

        patch_urlopen = patch(
            'urllib2.urlopen',
            Mock(side_effect=lambda *args, **kwargs: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        expected_metrics = {
            'job_resources_used_disk_percent': 20.0,
            'job_resources_used_disk_mb': 2
        }

        self.assertPublishedMany(publish_mock, expected_metrics)

    @patch.object(Collector, 'publish')
    def test_task_direct_disk_metrics(self, publish_mock):
        config = get_collector_config('MesosCollector', {
            'hosts': 'localhost:8081',
            'collect_disk_usage': True
        })

        self.collector = MesosCollector(config, None)

        fixtures = [StringIO("{}"),
                    StringIO(json.dumps(self.get_state(0, 22, 1))),
                    self.getFixture('slave_perf_3')]

        patch_urlopen = patch(
            'urllib2.urlopen',
            Mock(side_effect=lambda *args, **kwargs: fixtures.pop(0)))

        patch_communicate = patch(
            'subprocess.Popen.communicate',
            Mock(return_value=('4096\t/mnt/path\n', '')))

        patch_communicate.start()
        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()
        patch_communicate.stop()

        expected_metrics = {
            'job_resources_used_disk_percent': 25.0,
            'job_resources_used_disk_mb': 4
        }

        self.assertPublishedMany(publish_mock, expected_metrics)

    @patch.object(Collector, 'publish')
    def test_unit_conversion(self, publish_mock):
        config = get_collector_config('MesosCollector', {
            'hosts': 'localhost:8081',
            'collect_disk_usage': False,
            'byte_unit': ['kb']
        })

        self.collector = MesosCollector(config, None)

        fixtures = [StringIO("{}"),
                    StringIO(json.dumps(self.get_state(0, 24, 0))),
                    self.getFixture('slave_perf_4')]

        patch_urlopen = patch(
            'urllib2.urlopen',
            Mock(side_effect=lambda *args, **kwargs: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        expected_metrics = {
            'job_resources_used_disk_percent': 25.0,
            'job_resources_used_disk_kb': 4096,
            'job_resources_used_mem_anon_kb': 48516,
            'job_resources_used_mem_cache_kb': 9236,
            'job_resources_used_mem_critical_pressure_counter': 0,
            'job_resources_used_mem_file_kb': 9236,
            'job_resources_used_mem_limit_kb': 1179648,
            'job_resources_used_mem_low_pressure_counter': 0,
            'job_resources_used_mem_mapped_file_kb': 3808,
            'job_resources_used_mem_medium_pressure_counter': 0,
            'job_resources_used_mem_rss_kb': 48516,
            'job_resources_used_mem_swap_kb': 0,
            'job_resources_used_mem_total_kb': 58204,
            'job_resources_used_mem_unevictable_kb': 0,
        }

        self.assertPublishedMany(publish_mock, expected_metrics)

    def test_test(self):
        major, minor, point = MesosCollector.get_mesos_version(self.STATE)
        self.assertEqual(major, 0)
        self.assertEqual(minor, 22)
        self.assertEqual(point, 1)

################################################################################
if __name__ == "__main__":
    unittest.main()
