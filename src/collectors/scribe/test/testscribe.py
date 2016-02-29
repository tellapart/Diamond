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

from scribe import ScribeCollector

################################################################################

class TestScribeCollector(CollectorTestCase):

    def setUp(self):
        config = get_collector_config('ScribeCollector', {
            'ports': '1464',
            'bin': 'true'
        })

        self.collector = ScribeCollector(config, None)

    @patch.object(Collector, 'publish')
    def test_metrics(self, publish_mock):
        fixtures = [
            (self.getFixture('response1.txt').getvalue(), ''),
            (self.getFixture('response2.txt').getvalue(), '')]

        patch_communicate = patch(
            'subprocess.Popen.communicate',
            Mock(side_effect=lambda *args, **kwargs: fixtures.pop(0)))

        patch_communicate.start()
        self.collector.collect()
        # Clear the queue
        self.assertPublishedMany(publish_mock, {})
        self.collector.collect()
        patch_communicate.stop()

        published_metrics = {
            'denied_queue_size.notification': 0.0,
            'received_good.notification': 2.0,
            'received_good.scribe_overall': 500000.0,
            'denied_queue_size.scribe_overall': 10000.0
        }

        self.assertPublishedMany(publish_mock, published_metrics)

################################################################################
if __name__ == "__main__":
    unittest.main()
