#!/usr/bin/python
# coding=utf-8
################################################################################

from test import unittest
from test import run_only
import configobj

from diamond.handler.libratohandler import LibratoHandler
from diamond.metric import Metric


def run_only_if_librato_is_available(func):
    try:
        import librato
        librato  # workaround for pyflakes issue #13
    except ImportError:
        librato = None
    pred = lambda: librato is not None
    return run_only(func, pred)


class TestLibratoHandler(unittest.TestCase):

    METRIC = Metric('servers.com.example.www.cpu.total.idle',
                    0,
                    timestamp=1234567,
                    host='com.example.www',
                    interval=60)

    @run_only_if_librato_is_available
    def test_metric_to_librato_item(self):
        config = configobj.ConfigObj()

        handler = LibratoHandler(config)

        item = handler._get_queue_item(self.METRIC)

        self.assertEqual(item, {
            'name': 'cpu.total.idle',
            'value': 0.0,
            'source': 'com.example.www',
            'measure_time': 1234567,
            'type': 'counter'
        })

    @run_only_if_librato_is_available
    def test_metric_to_librato_item_additional_data(self):
        config = configobj.ConfigObj()
        config['include_period'] = True
        config['enable_ssa'] = True

        handler = LibratoHandler(config)

        item = handler._get_queue_item(self.METRIC)

        self.assertEqual(item, {
            'name': 'cpu.total.idle',
            'value': 0.0,
            'source': 'com.example.www',
            'measure_time': 1234567,
            'type': 'counter',
            'period': 60,
            'attributes': {
                'aggregate': True
            }
        })
