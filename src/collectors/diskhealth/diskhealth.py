# coding=utf-8

"""
Check Disk Health

Note: The user running Diamond will need to have write access to each mount point
that should be checked. The test performa a write and a read in each mount.

#### Dependencies

 * /proc/mount

"""

import diamond.convertor

import time
import os
import re

from multiprocessing import Pool, TimeoutError

TEST_FILE_SIZE_BYTES = 1048576  # 1MB


def _format_metric(device, mount_point, metric):
    return '%s.%s.%s' % (
        device.strip('/').replace('/','_'),
        'root' if mount_point == '/' else mount_point.strip('/').replace('/','_'),
        metric)


def _test_disk(device, mount_point):
    metrics = {}
    filename = os.path.join(mount_point, '.diamond_diskhealth')

    data = os.urandom(TEST_FILE_SIZE_BYTES)
    start_time = time.time()
    try:
        f = open(filename, 'wb')
        f.write(data)
        f.flush()
        f.close()
        write_time = time.time()
        metrics[_format_metric(device, mount_point, 'write_error')] = 0
        metrics[_format_metric(device, mount_point, 'write_time')] = write_time  - start_time
        try:
            f = open(filename, 'r')
            read_data = f.read()
            f.close()
            read_time = time.time()
            metrics[_format_metric(device, mount_point, 'read_error')] = 0 if read_data == data else 1
            metrics[_format_metric(device, mount_point, 'read_time')] = read_time - write_time
        except Exception as e:
            metrics[_format_metric(device, mount_point, 'read_error')] = 1
    except Exception as e:
        metrics[_format_metric(device, mount_point, 'write_error')] = 1
    return metrics


class DiskHealthCollector(diamond.collector.Collector):

    MAX_THREADS = 5
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
                       + " Defaults to btrfs, ext2, ext3, ext4, xfs."
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
            'fs_types': ','.join(self.SUPPORTED_FS_TYPES)
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

        if os.access('/proc/mounts', os.R_OK):
            fp = open('/proc/mounts')
            for line in fp:
                columns = line.split()
                device = columns[0]
                mount_point = columns[1]
                fs_type = columns[2]

                if not reg.match(os.path.relpath(device, '/dev')):
                    continue

                if fs_type not in fs_types:
                    continue

                result[device] = mount_point
            fp.close()
        return result

    def collect(self):
        disks = self.get_disks()
        pool = Pool(min(len(disks), self.MAX_THREADS))
        prcs_async = {pool.apply_async(_test_disk,
                args=[device, mount_point]): [device, mount_point]
                for device, mount_point in disks.items()}
        results = {}
        for p, args in prcs_async.items():
            try:
                results.update(p.get(self.TEST_TIMEOUT_SEC))
                results[_format_metric(args[0], args[1], 'timeout')] = 0
            except TimeoutError as e:
                results[_format_metric(args[0], args[1], 'timeout')] = 1

        pool.close()
        for k, v in results.items():
            self.publish(k, v)
