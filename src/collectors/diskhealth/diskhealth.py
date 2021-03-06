# coding=utf-8

"""
Check Disk Health

Note: The user running Diamond will need to have write access to each mount point
that should be checked. The test performa a write and a read in each mount.

#### Dependencies

 * /proc/mount

"""

from diamond.collector import str_to_bool
import diamond.convertor

import time
import os
import re

from multiprocessing import Pool, TimeoutError


def _format_metric(device, mount_point, metric, raw_stats_only):
    mount = 'root' if mount_point == '/' else mount_point.strip('/')
    if raw_stats_only:
        return '%s/%s/%s' % (device, mount, metric)
    return '%s.%s.%s' % (device, mount.replace('/', '_'), metric)


def _test_disk(device, mount_point, file_name, file_size, raw_stats_only):
    metrics = {}
    filename = os.path.join(mount_point, file_name)

    data = os.urandom(file_size)
    start_time = time.time()
    message = None
    wf = None
    try:
        wf = open(filename, 'wb')
        wf.write(data)
        wf.flush()
        wf.close()
        write_time = time.time()
        metrics[_format_metric(device,
                mount_point,
                'write_error',
                raw_stats_only)] = 0
        metrics[_format_metric(device,
                mount_point,
                'write_time_ms',
                raw_stats_only)] = (write_time - start_time) * 1000
        rf = None
        try:
            rf = open(filename, 'r')
            read_data = rf.read()
            read_time = time.time()
            metrics[_format_metric(device,
                    mount_point,
                    'read_error',
                    raw_stats_only)] = 0 if read_data == data else 1
            metrics[_format_metric(device,
                    mount_point,
                    'read_time_ms',
                    raw_stats_only)] = (read_time - write_time) * 1000
        except Exception as e:
            message = 'Read error on %s %s \n %s' % (device, mount_point, e)
            metrics[_format_metric(device,
                    mount_point,
                    'read_error',
                    raw_stats_only)] = 1
        finally:
            if rf:
              rf.close()
    except Exception as e:
        message = 'Write error on %s %s \n %s' % (device, mount_point, e)
        metrics[_format_metric(device,
                mount_point,
                'read_error',
                raw_stats_only)] = 1
        metrics[_format_metric(device,
                mount_point,
                'write_error',
                raw_stats_only)] = 1
    finally:
      if wf:
        wf.close()
    return metrics, message


class DiskHealthCollector(diamond.collector.Collector):

    MAX_THREADS = 5
    TEST_FILE_NAME = '.diamond_diskhealth'
    TEST_FILE_SIZE = 1048576  # 1MB
    TEST_TIMEOUT_SEC = 5
    SUPPORTED_FS_TYPES = [
        'btrfs',
        'ext2',
        'ext3',
        'ext4',
        'xfs'
    ]

    def __init__(self, config, handlers):
        super(DiskHealthCollector, self).__init__(config, handlers)

    def get_default_config_help(self):
        config_help = super(DiskHealthCollector, self).get_default_config_help()
        config_help.update({
            'devices': "A regex of which devices to gather metrics for."
                       + " Defaults to md, sd, xvd, disk, and dm devices",
            'fs_types': "A list of fs types for which to perform the health test"
                       + " Defaults to btrfs, ext2, ext3, ext4, xfs.",
            'raw_stats_only': "If True will not format device names",
            'test_file_name': "The name of the test file that is written."
                       + " Defaults to '.diamond_diskhealth'",
            'test_file_size': "The size of the test file in bytes that is written"
                       + " and read during execution. Defaults to 1048576 (1MB)."
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(DiskHealthCollector, self).get_default_config()
        config.update({
            'enabled':  'True',
            'devices':  ('PhysicalDrive[0-9]+$'
                         + '|md[0-9]+$'
                         + '|sd[a-z]+[0-9]*$'
                         + '|x?vd[a-z]+[0-9]*$'
                         + '|disk[0-9]+$'
                         + '|dm\-[0-9]+$'),
            'fs_types': ','.join(self.SUPPORTED_FS_TYPES),
            'raw_stats_only': False,
            'test_file_name': self.TEST_FILE_NAME,
            'test_file_size': self.TEST_FILE_SIZE
        })
        return config

    def get_disks(self):
        """
        Create a map of disks in the machine.

        Returns:
          { device: mount_point }
        """
        result = {}

        exp = self.config['devices']
        reg = re.compile(exp)
        fs_types = set(self.config['fs_types'].split(','))

        try:
          fp = open('/proc/mounts')
          for line in fp:
              columns = line.split()
              device = columns[0].strip('/').replace('dev/','',1)
              mount_point = columns[1]
              fs_type = columns[2]

              if not reg.match(device):
                  continue

              if fs_type not in fs_types:
                  continue

              result[device] = mount_point
        except Exception as e:
            self.log.debug('Could not read /proc/mounts!')
            self.log.exception(e)
        finally:
            fp.close()
        return result

    def collect(self):
        raw_stats_only = str_to_bool(self.config['raw_stats_only'])
        test_file_name = self.config['test_file_name']
        test_file_size = self.config['test_file_size']
        disks = self.get_disks()
        pool = Pool(min(len(disks), self.MAX_THREADS))
        prcs_async = {pool.apply_async(_test_disk,
                args=[device, mount_point, test_file_name, test_file_size, raw_stats_only]):
                [device, mount_point] for device, mount_point in disks.items()}
        results = {}
        for p, args in prcs_async.items():
            try:
                result, message  = p.get(self.TEST_TIMEOUT_SEC)
                if message :
                    self.log.debug(message)
                results.update(result)
                results[_format_metric(args[0], args[1], 'timeout_error', raw_stats_only)] = 0
            except TimeoutError as e:
                results[_format_metric(args[0], args[1], 'timeout_error', raw_stats_only)] = 1
                self.log.debug('Writing to disk timed out %s %s.', args[0], args[1])
        pool.close()
        for k, v in results.items():
            self.publish(k, v)
