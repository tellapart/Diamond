#!/usr/bin/python
# coding=utf-8
################################################################################

from mock import patch, Mock
from test import unittest
from test import run_only
import configobj

from diamond.handler.observability import ObservabilityHandler
from diamond.metric import Metric


def run_only_if_requests_is_available(func):
    try:
        from oauthlib import oauth2
        import requests_oauthlib
    except ImportError:
        oauth2 = None
        requests_oauthlib = None
    pred = lambda: oauth2 is not None and requests_oauthlib is not None
    return run_only(func, pred)


class TestObservabilityHandler(unittest.TestCase):

    METRICS = [
        Metric(
            path='servers.com.example.www.cpu.total.idle',
            value=1.4,
            timestamp=1234567,
            host='com.example.www'),
        Metric(
            path='servers.com.example.www.cpu.total.user',
            value=5.4,
            timestamp=1234567,
            host='com.example.www'),
        Metric(
            path='servers.com.example.www.cpu.total.user',
            value=5.4,
            timestamp=1234547,
            host='com.example.www'),
        Metric(
            path='servers.com.example.www2.cpu.total.user',
            value=5.4,
            timestamp=1234547,
            host='com.example.www2'),
        Metric(
            path='servers.com.example.www.cpu.total.user',
            value=5.4,
            timestamp=1234547,
            host='com.example.www'),
    ]

    @run_only_if_requests_is_available
    def test_metric_to_obs_item(self):
        config = configobj.ConfigObj()
        handler = ObservabilityHandler(config)

        item = handler._get_queue_item(self.METRICS[0])

        self.assertEqual(item, {
            'service': 'cpu',
            'source': 'com.example.www',
            'name': 'total.idle',
            'value': 1.4,
            'measure_time': 1234567,
        })

    @run_only_if_requests_is_available
    def test_metric_grouping(self):
        config = configobj.ConfigObj()
        handler = ObservabilityHandler(config)

        items = [handler._get_queue_item(m) for m in self.METRICS]
        groups = handler._create_request_groups(items)

        self.assertEqual(
            set(groups.keys()),
            {('cpu', 'com.example.www2', 1234540),
             ('cpu', 'com.example.www', 1234560),
             ('cpu', 'com.example.www', 1234540)}
        )

        self.assertEqual(len(groups[('cpu', 'com.example.www2', 1234540)]), 1)
        self.assertEqual(len(groups[('cpu', 'com.example.www', 1234560)]), 2)
        self.assertEqual(len(groups[('cpu', 'com.example.www', 1234540)]), 2)

    @run_only_if_requests_is_available
    def test_metric_to_obs_expiration(self):
        config = configobj.ConfigObj()
        config['queue_max_age'] = 900

        metric = Metric(
            path='servers.com.example.www.cpu.total.idle',
            value=0,
            timestamp=0,
            host='com.example.www')

        handler = ObservabilityHandler(config)
        handler.process(metric)

        with patch('time.time', Mock(return_value=10)):
            items = handler._get_valid_items()
            self.assertEqual(len(items), 1)

        with patch('time.time', Mock(return_value=900)):
            items = handler._get_valid_items()
            self.assertEqual(len(items), 0)
