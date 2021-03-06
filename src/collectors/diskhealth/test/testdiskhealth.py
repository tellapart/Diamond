#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import Mock
from mock import patch
from time import sleep

from diamond.collector import Collector
from diskhealth import DiskHealthCollector, _test_disk

################################################################################


class TestDiskHealtCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('DiskHealthCollector', {
        })

        self.collector = DiskHealthCollector(config, None)

    def test_import(self):
        self.assertTrue(DiskHealthCollector)

    @patch('os.access', Mock(return_value=True))
    def test_get_mounts(self):

        patch_open = patch(
             '__builtin__.open',
             Mock(return_value=self.getFixture('mounts')))

        open_mock = patch_open.start()
        result = self.collector.get_disks()
        patch_open.stop()

        self.assertTrue(result.get('md0') == '/mnt')
        self.assertTrue(result.get('xvda1') == '/')

        self.assertEqual(len(result), 2)

        open_mock.assert_called_once_with('/proc/mounts')

        return result

    class MockMemmoryFile(object):
        CANT_WRITE = 1
        CANT_READ = 2
        BAD_READ = 3
        TIMEOUT = 4

        def __init__(self, mode=0):
          self.data = None
          self.mode = mode

        def write(self, data):
          if self.mode == self.CANT_WRITE:
            raise Exception('Cant write')
          elif self.mode == self.TIMEOUT:
            sleep(20)
          self.data = data

        def read(self):
          if self.mode == self.CANT_READ:
            raise Exception('Cant read')
          if self.mode == self.BAD_READ:
            return 'BAD READ'
          return self.data

        def close(self):
          pass

        def flush(self):
          pass


    def test_disk_ok(self):
        patch_open = patch(
             '__builtin__.open',
             Mock(return_value=self.MockMemmoryFile()))

        open_mock = patch_open.start()
        result, message = _test_disk('md0',
                'mnt',
                self.collector.TEST_FILE_NAME,
                self.collector.TEST_FILE_SIZE,
                False)
        patch_open.stop()

        self.assertTrue(result.get('md0.mnt.write_time_ms') >= 0)
        self.assertTrue(result.get('md0.mnt.read_time_ms') >= 0)
        self.assertTrue(result.get('md0.mnt.read_error') == 0)
        self.assertTrue(result.get('md0.mnt.write_error') == 0)
        return result

    def test_disk_cant_write(self):
        patch_open = patch(
             '__builtin__.open',
             Mock(return_value=self.MockMemmoryFile(self.MockMemmoryFile.CANT_WRITE)))
        open_mock = patch_open.start()
        result, message = _test_disk('md0',
                'mnt',
                self.collector.TEST_FILE_NAME,
                self.collector.TEST_FILE_SIZE,
                False)
        patch_open.stop()

        self.assertTrue(result.get('md0.mnt.write_time_ms') == None)
        self.assertTrue(result.get('md0.mnt.read_time_ms') == None)
        self.assertTrue(result.get('md0.mnt.read_error') == 1)
        self.assertTrue(result.get('md0.mnt.write_error') == 1)
        return result
    #
    def test_disk_cant_read(self):
        patch_open = patch(
             '__builtin__.open',
             Mock(return_value=self.MockMemmoryFile(self.MockMemmoryFile.CANT_READ)))
        open_mock = patch_open.start()
        result, message = _test_disk('md0',
                'mnt',
                self.collector.TEST_FILE_NAME,
                self.collector.TEST_FILE_SIZE,
                False)
        patch_open.stop()

        self.assertTrue(result.get('md0.mnt.write_time_ms') >= 0)
        self.assertTrue(result.get('md0.mnt.read_time_ms') == None)
        self.assertTrue(result.get('md0.mnt.read_error') == 1)
        self.assertTrue(result.get('md0.mnt.write_error') == 0)
        return result

    def test_disk_bad_read(self):
        patch_open = patch(
             '__builtin__.open',
             Mock(return_value=self.MockMemmoryFile(self.MockMemmoryFile.BAD_READ)))
        open_mock = patch_open.start()
        result, message = _test_disk('md0',
                'mnt',
                self.collector.TEST_FILE_NAME,
                self.collector.TEST_FILE_SIZE,
                True)
        patch_open.stop()

        self.assertTrue(result.get('md0/mnt/write_time_ms') >= 0)
        self.assertTrue(result.get('md0/mnt/read_time_ms') >= 0)
        self.assertTrue(result.get('md0/mnt/read_error') == 1)
        self.assertTrue(result.get('md0/mnt/write_error') == 0)
        return result

    @patch.object(Collector, 'publish')
    def test_diskhealth_collector(self, publish_mock):
        self.collector.get_disks = lambda: {'md0': 'mnt', 'xvda1': 'root'}
        patch_open = patch(
             '__builtin__.open',
             Mock(return_value=self.MockMemmoryFile()))
        open_mock = patch_open.start()
        self.collector.collect()
        patch_open.stop()

        metrics = {
          'md0.mnt.read_error': 0,
          'md0.mnt.write_error': 0,
          'md0.mnt.timeout_error': 0,
          'xvda1.root.read_error': 0,
          'xvda1.root.write_error': 0,
          'xvda1.root.timeout_error': 0
        }

        self.assertPublishedMany(publish_mock, metrics)

    @patch.object(Collector, 'publish')
    def test_diskhealth_collector_timout(self, publish_mock):
        self.collector.TEST_TIMEOUT_SEC = 1
        self.collector.get_disks = lambda: {'md0': 'mnt', 'xvda1': 'root'}
        patch_open = patch(
             '__builtin__.open',
             Mock(return_value=self.MockMemmoryFile(self.MockMemmoryFile.TIMEOUT)))
        open_mock = patch_open.start()
        self.collector.collect()
        patch_open.stop()
        metrics = {
          'md0.mnt.timeout_error': 1,
          'xvda1.root.timeout_error': 1
        }
        self.assertPublishedMany(publish_mock, metrics)

################################################################################
if __name__ == "__main__":
    unittest.main()
