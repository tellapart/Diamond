#!/usr/bin/python
# coding=utf-8
###############################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from multifiles import MultiFilesCollector


###############################################################################

class TestMultiFilesCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('MultiFilesCollector', {
        })
        self.collector = MultiFilesCollector(config, None)

    def test_import(self):
        self.assertTrue(MultiFilesCollector)

###############################################################################
if __name__ == "__main__":
    unittest.main()
