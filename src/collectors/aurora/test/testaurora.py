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
from mock import call
from mock import Mock
from mock import patch

from diamond.collector import Collector

from aurora import AuroraCollector

################################################################################


class TestAuroraCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('AuroraCollector', {
            'hosts': 'localhost:8081',
        })

        self.collector = AuroraCollector(config, None)

    def test_import(self):
        self.assertTrue(TestAuroraCollector)

    @patch.object(Collector, 'publish')
    def test_cluster_vars(self, publish_mock):
        fixtures = [
            self.getFixture('role_summary'),
            self.getFixture('cluster_vars'),
            self.getFixture('job_summary')
        ]
        patch_urlopen = patch('urllib2.OpenerDirector.open',
                              Mock(side_effect=lambda *args: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        published_metrics = {
            'schedule_queue_size': 0,
            'scheduler_thrift_createJob_nanos_total': 40390494,
            'task_throttle_events': 258
        }

        self.assertPublishedMany(publish_mock, published_metrics)

        unpublished_metrics = {
            'jvm_prop_java_class_path': 0,
            'system_env_TERM': 0
        }

        self.assertUnpublishedMany(publish_mock, unpublished_metrics)

    @patch.object(Collector, 'publish')
    def test_no_publish_if_inactive(self, publish_mock):
        metrics = {
            'schedule_queue_size': 0,
            'scheduler_thrift_createJob_nanos_total': 40390494,
            'task_throttle_events': 258
        }
        fixtures = [
            self.getFixture('role_summary'),
            StringIO(json.dumps(metrics)),
            self.getFixture('job_summary')
        ]
        patch_urlopen = patch('urllib2.OpenerDirector.open',
                              Mock(side_effect=lambda *args: fixtures.pop(0)))

        patch_urlopen.start()
        self.collector.collect()
        patch_urlopen.stop()

        self.assertUnpublishedMany(publish_mock, metrics)

    @patch.object(Collector, 'publish')
    def test_extract_metric_and_source(self, publish_mock):
        metrics = {
            'tasks_FAILED_ubuntu/devel/test.other': 123,
            'sla_test_role/prod/my_job_job_uptime_90.00_sec': 1143469,
            'scheduler_lifecycle_ACTIVE': 1
        }
        fixtures = [
            self.getFixture('role_summary'),
            StringIO(json.dumps(metrics)),
            self.getFixture('job_summary'),
            self.getFixture('role_summary'),
            StringIO(json.dumps(metrics)),
            self.getFixture('job_summary')
        ]
        patch_urlopen = patch('urllib2.OpenerDirector.open',
                              Mock(side_effect=lambda *args: fixtures.pop(0)))

        config = get_collector_config('AuroraCollector', {
            'hosts': 'test@localhost:8081',
        })

        cluster_override_collector = AuroraCollector(config, None)
        collectors = [
            ('test', cluster_override_collector),
            ('general', self.collector)
        ]

        for cluster, collector in collectors:
            patch_urlopen.start()
            collector.collect()
            patch_urlopen.stop()

            published_metrics = {
                'sla_job_job_uptime_90_00_sec':
                    call('sla_job_job_uptime_90_00_sec', 1143469.0,
                         metric_type='GAUGE',
                         source='%s.test_role.prod.my_job' % cluster),
                'job_stats_active_tasks':
                    call('job_stats_active_tasks', 1,
                         metric_type='GAUGE',
                         source='%s.test_role.prod.my_job' % cluster),
                'job_stats_failed_tasks':
                    call('job_stats_failed_tasks', 1,
                         metric_type='COUNTER',
                         source='%s.test_role.prod.my_job' % cluster),
                'job_stats_finished_tasks':
                    call('job_stats_finished_tasks', 4,
                         metric_type='COUNTER',
                         source='%s.test_role.prod.my_job' % cluster),
                'job_resources_total_allocated_cpu':
                    call('job_resources_total_allocated_cpu', 17,
                         metric_type='GAUGE',
                         source='%s.test_role.prod.my_job' % cluster),
            }

            for k, v in published_metrics.iteritems():
                calls = filter(lambda x: x[0][0] == k,
                               publish_mock.call_args_list)
                self.assertEqual(calls[0], v)

            publish_mock.reset_mock()

################################################################################
if __name__ == "__main__":
    unittest.main()
