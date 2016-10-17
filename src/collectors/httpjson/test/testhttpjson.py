#!/usr/bin/python
# coding=utf-8
################################################################################
from test import CollectorTestCase
from test import get_collector_config
from mock import Mock
from mock import patch
from diamond.collector import Collector
from httpjson import HTTPJSONCollector

################################################################################


class TestHTTPJSONCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('HTTPJSONCollector', {})
        self.collector = HTTPJSONCollector(config, None)

    def test_import(self):
        self.assertTrue(HTTPJSONCollector)

    @patch.object(Collector, 'publish')
    def test_should_work_with_real_data(self, publish_mock):
        urlopen_mock = patch('urllib2.urlopen',
                             Mock(return_value=self.getFixture('stats.json')))

        urlopen_mock.start()
        self.collector.collect()
        urlopen_mock.stop()

        metrics = self.getPickledResults("real_stat.pkl")

        self.assertPublishedMany(publish_mock, metrics)

    def _test_metrics(self, raw, expected, prefix=''):
        flattened = sorted(self.collector._json_to_flat_metrics(prefix, raw))
        self.assertEqual(flattened, expected)

    def test_flat_metrics_basic(self):
        raw = {
            'key1': 100,
            'key2': 200
        }
        expected = [('key1', 100), ('key2', 200)]
        self._test_metrics(raw, expected)

    def test_flat_metrics_numeric(self):
        raw = {
            'float': 1.23,
            'int': 100
        }
        expected = [('float', 1.23), ('int', 100)]
        self._test_metrics(raw, expected)

    def test_flat_metrics_junk(self):
        raw = {
            'junk': 'junk',
            'real': 100
        }
        expected = [('junk', None), ('real', 100)]
        self._test_metrics(raw, expected)

    def test_flat_metrics_nested(self):
        raw = {
            'parent': {
                'child': 100
            }
        }
        expected = [('parent.child', 100)]
        self._test_metrics(raw, expected)

    def test_flat_metrics_prefix(self):
        raw = {
            'parent': {
                'child': 100
            }
        }
        expected = [('grandparent.parent.child', 100)]
        self._test_metrics(raw, expected, 'grandparent')
