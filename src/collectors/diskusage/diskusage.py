# coding=utf-8

"""
Collect IO Stats

Note: You may need to artifically generate some IO load on a disk/partition
before graphite will generate the metrics.

 * http://www.kernel.org/doc/Documentation/iostats.txt

#### Dependencies

 * /proc/diskstats

"""

import diamond.collector
from diamond.collector import str_to_bool
import diamond.convertor
import time
import os
import re

try:
    import psutil
    psutil  # workaround for pyflakes issue #13
except ImportError:
    psutil = None


class DiskUsageCollector(diamond.collector.Collector):

    READS = 'reads'
    READS_MERGED = 'reads_merged'
    READS_SECTORS = 'reads_sectors'
    READS_MILLISECONDS = 'reads_milliseconds'
    WRITES = 'writes'
    WRITES_MERGED = 'writes_merged'
    WRITES_SECTORS = 'writes_sectors'
    WRITES_MILLISECONDS = 'writes_milliseconds'
    IO_IN_PROGRESS = 'io_in_progress'
    IO_MILLISECONDS = 'io_milliseconds'
    IO_MILLISECONDS_WEIGHTED = 'io_milliseconds_weighted'

    MAX_VALUES = {
        READS:                    4294967295,
        READS_MERGED:             4294967295,
        READS_MILLISECONDS:       4294967295,
        WRITES:                   4294967295,
        WRITES_MERGED:            4294967295,
        WRITES_MILLISECONDS:      4294967295,
        IO_MILLISECONDS:          4294967295,
        IO_MILLISECONDS_WEIGHTED: 4294967295
    }

    RAW_METRIC_NAMES = {
        READS: 'num_reads',
        READS_MERGED: 'merged_reads',
        READS_SECTORS: 'sectors_read',
        READS_MILLISECONDS: 'ms_read',
        WRITES: 'num_writes',
        WRITES_MERGED: 'merged_writes',
        WRITES_SECTORS: 'sectors_written',
        WRITES_MILLISECONDS: 'ms_written',
        IO_IN_PROGRESS: 'io_inprogress',
        IO_MILLISECONDS: 'ms_doing_io',
        IO_MILLISECONDS_WEIGHTED: 'ms_doing_io_weighted',
    }

    LastCollectTime = None

    def __init__(self, config, handlers):
        super(DiskUsageCollector, self).__init__(config, handlers)
        self.raw_stats_only = str_to_bool(self.config['raw_stats_only'])

    def get_default_config_help(self):
        config_help = super(DiskUsageCollector, self).get_default_config_help()
        config_help.update({
            'devices': "A regex of which devices to gather metrics for."
                       + " Defaults to md, sd, xvd, disk, and dm devices",
            'sector_size': 'The size to use to calculate sector usage',
            'send_zero': 'Send io data even when there is no io',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(DiskUsageCollector, self).get_default_config()
        config.update({
            'enabled':  'True',
            'path':     'iostat',
            'devices':  ('PhysicalDrive[0-9]+$'
                         + '|md[0-9]+$'
                         + '|sd[a-z]+[0-9]*$'
                         + '|x?vd[a-z]+[0-9]*$'
                         + '|disk[0-9]+$'
                         + '|dm\-[0-9]+$'),
            'sector_size': 512,
            'send_zero': False,
            'raw_stats_only': False
        })
        return config

    def get_metric_name(self, name):
        if self.raw_stats_only:
            return self.RAW_METRIC_NAMES[name]

        return name

    def get_disk_statistics(self):
        """
        Create a map of disks in the machine.

        http://www.kernel.org/doc/Documentation/iostats.txt

        Returns:
          (major, minor) -> DiskStatistics(device, ...)
        """
        result = {}

        if os.access('/proc/diskstats', os.R_OK):
            self.proc_diskstats = True
            fp = open('/proc/diskstats')

            try:
                for line in fp:
                    try:
                        columns = line.split()
                        # On early linux v2.6 versions, partitions have only 4
                        # output fields not 11. From linux 2.6.25 partitions
                        # have the full stats set.
                        if len(columns) < 14:
                            continue
                        major = int(columns[0])
                        minor = int(columns[1])
                        device = columns[2]

                        if (device.startswith('ram')
                                or device.startswith('loop')):
                            continue

                        result[(major, minor)] = {
                            'device': device,
                            self.get_metric_name(self.READS): float(columns[3]),
                            self.get_metric_name(self.READS_MERGED): float(columns[4]),
                            self.get_metric_name(self.READS_SECTORS): float(columns[5]),
                            self.get_metric_name(self.READS_MILLISECONDS): float(columns[6]),
                            self.get_metric_name(self.WRITES): float(columns[7]),
                            self.get_metric_name(self.WRITES_MERGED): float(columns[8]),
                            self.get_metric_name(self.WRITES_SECTORS): float(columns[9]),
                            self.get_metric_name(self.WRITES_MILLISECONDS): float(columns[10]),
                            self.get_metric_name(self.IO_IN_PROGRESS):  float(columns[11]),
                            self.get_metric_name(self.IO_MILLISECONDS): float(columns[12]),
                            self.get_metric_name(self.IO_MILLISECONDS_WEIGHTED): float(columns[13])
                        }
                    except ValueError:
                        continue
            finally:
                fp.close()
        else:
            self.proc_diskstats = False
            if not psutil:
                self.log.error('Unable to import psutil')
                return None

            disks = psutil.disk_io_counters(True)
            for disk in disks:
                    result[(0, len(result))] = {
                        'device': disk,
                        'reads': disks[disk].read_count,
                        'reads_sectors': (disks[disk].read_bytes
                                          / int(self.config['sector_size'])),
                        'reads_milliseconds': disks[disk].read_time,
                        'writes': disks[disk].write_count,
                        'writes_sectors': (disks[disk].write_bytes
                                           / int(self.config['sector_size'])),
                        'writes_milliseconds': disks[disk].write_time,
                        'io_milliseconds':
                        disks[disk].read_time + disks[disk].write_time,
                        'io_milliseconds_weighted':
                        disks[disk].read_time + disks[disk].write_time
                    }

        return result

    def collect(self):

        # Handle collection time intervals correctly
        CollectTime = time.time()
        time_delta = float(self.config['interval'])
        if self.LastCollectTime:
            time_delta = CollectTime - self.LastCollectTime
        if not time_delta:
            time_delta = float(self.config['interval'])
        self.LastCollectTime = CollectTime

        exp = self.config['devices']
        reg = re.compile(exp)

        results = self.get_disk_statistics()
        if not results:
            self.log.error('No diskspace metrics retrieved')
            return None

        for key, info in results.iteritems():
            metrics = {}
            name = info['device']
            if not reg.match(name):
                continue

            if self.raw_stats_only:
                for k, v in info.iteritems():
                    if k == 'device':
                        continue
                    metric_name = '/'.join((name, k))
                    self.publish(metric_name, v)
                continue

            for key, value in info.iteritems():
                if key == 'device':
                    continue
                oldkey = key

                for unit in self.config['byte_unit']:
                    key = oldkey

                    if key.endswith('sectors'):
                        key = key.replace('sectors', unit)
                        value /= (1024 / int(self.config['sector_size']))
                        value = diamond.convertor.binary.convert(value=value,
                                                                 oldUnit='kB',
                                                                 newUnit=unit)
                        self.MAX_VALUES[key] = diamond.convertor.binary.convert(
                            value=diamond.collector.MAX_COUNTER,
                            oldUnit='byte',
                            newUnit=unit)

                    metric_name = '.'.join([info['device'], key])
                    # io_in_progress is a point in time counter, !derivative
                    if key != 'io_in_progress':
                        metric_value = self.derivative(
                            metric_name,
                            value,
                            self.MAX_VALUES[key],
                            time_delta=False)
                    else:
                        metric_value = value

                    metrics[key] = metric_value

            if self.proc_diskstats:
                metrics['read_requests_merged_per_second'] = (
                    metrics['reads_merged'] / time_delta)
                metrics['write_requests_merged_per_second'] = (
                    metrics['writes_merged'] / time_delta)

            metrics['reads_per_second'] = metrics['reads'] / time_delta
            metrics['writes_per_second'] = metrics['writes'] / time_delta

            for unit in self.config['byte_unit']:
                metric_name = 'read_%s_per_second' % unit
                key = 'reads_%s' % unit
                metrics[metric_name] = metrics[key] / time_delta

                metric_name = 'write_%s_per_second' % unit
                key = 'writes_%s' % unit
                metrics[metric_name] = metrics[key] / time_delta

                # Set to zero so the nodes are valid even if we have 0 io for
                # the metric duration
                metric_name = 'average_request_size_%s' % unit
                metrics[metric_name] = 0

            metrics['io'] = metrics['reads'] + metrics['writes']

            metrics['average_queue_length'] = (
                metrics['io_milliseconds_weighted']
                / time_delta
                / 1000.0)

            metrics['util_percentage'] = (metrics['io_milliseconds']
                                          / time_delta
                                          / 10.0)

            if metrics['reads'] > 0:
                metrics['read_await'] = (
                    metrics['reads_milliseconds'] / metrics['reads'])
            else:
                metrics['read_await'] = 0

            if metrics['writes'] > 0:
                metrics['write_await'] = (
                    metrics['writes_milliseconds'] / metrics['writes'])
            else:
                metrics['write_await'] = 0

            for unit in self.config['byte_unit']:
                rkey = 'reads_%s' % unit
                wkey = 'writes_%s' % unit
                metric_name = 'average_request_size_%s' % unit
                if (metrics['io'] > 0):
                    metrics[metric_name] = (
                        metrics[rkey] + metrics[wkey]) / metrics['io']
                else:
                    metrics[metric_name] = 0

            metrics['iops'] = metrics['io'] / time_delta

            if (metrics['io'] > 0):
                metrics['service_time'] = (
                    metrics['io_milliseconds'] / metrics['io'])
                metrics['await'] = (
                    metrics['reads_milliseconds']
                    + metrics['writes_milliseconds']) / metrics['io']
            else:
                metrics['service_time'] = 0
                metrics['await'] = 0

            # http://www.scribd.com/doc/15013525
            # Page 28
            metrics['concurrent_io'] = (metrics['reads_per_second']
                                        + metrics['writes_per_second']
                                        ) * (metrics['service_time']
                                             / 1000.0)

            # Only publish when we have io figures
            if (metrics['io'] > 0 or self.config['send_zero']):
                for key in metrics:
                    metric_name = '.'.join([info['device'], key]).replace(
                        '/', '_')
                    self.publish(metric_name, metrics[key])
