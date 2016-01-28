# coding=utf-8

"""
This class collects data on memory utilization

Note that MemFree may report no memory free. This may not actually be the case,
as memory is allocated to Buffers and Cache as well. See
[this link](http://www.linuxatemyram.com/) for more details.

#### Dependencies

* /proc/meminfo or psutil

"""

import diamond.collector
from diamond.collector import str_to_bool
import diamond.convertor
import os
import re

try:
    import psutil
    psutil  # workaround for pyflakes issue #13
except ImportError:
    psutil = None

_KEY_MAPPING = [
    'MemTotal',
    'MemFree',
    'Buffers',
    'Cached',
    'Active',
    'Dirty',
    'Inactive',
    'Shmem',
    'SwapTotal',
    'SwapFree',
    'SwapCached',
    'VmallocTotal',
    'VmallocUsed',
    'VmallocChunk',
    'Committed_AS',
    'MemPercentUsed'
]


class MemoryCollector(diamond.collector.Collector):

    PROC = '/proc/meminfo'

    def get_default_config_help(self):
        config_help = super(MemoryCollector, self).get_default_config_help()
        config_help.update({
            'detailed': 'Set to True to Collect all the nodes',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MemoryCollector, self).get_default_config()
        config.update({
            'enabled':  'True',
            'path':     'memory',
            'method':   'Threaded',
            # Collect all the nodes or just a few standard ones?
            # Uncomment to enable
            #'detailed': 'True',
            'lower_name': 'False'
        })
        return config

    def _calculate_percent_used(self, parts):
        """
        Calculate and return the current % of memory used.
        """
        try:
            buffers, _ = parts['Buffers']
            cached, _ = parts['Cached']
            memfree, _ = parts['MemFree']

            free = buffers + cached + memfree
            total = float(parts['MemTotal'][0])

            used = round(((total - free) / total) * 100, 2)
            return used, None
        except (KeyError, ZeroDivisionError):
            return 0, None

    def collect(self):
        """
        Collect memory stats
        """
        if os.access(self.PROC, os.R_OK):
            pattern = re.compile('(?P<name>\w+)\((?P<suffix>\w+)\)')
            file = open(self.PROC)
            data = file.read()
            file.close()

            parts = dict()
            for line in data.splitlines():
                try:
                    name, value, units = line.split()
                    name = name.rstrip(':')
                    m = pattern.search(name)
                    if m:
                        name = '_'.join((m.group('name'), m.group('suffix')))
                    if str_to_bool(self.config.get('lower_name')):
                        name = name.lower()

                    value = int(value)

                    parts[name] = (value, units)

                except ValueError:
                    continue

            parts['MemPercentUsed'] = self._calculate_percent_used(parts)

            for name, output in parts.iteritems():
                if (name not in _KEY_MAPPING
                        and 'detailed' not in self.config):
                    continue

                value, units = output
                for unit in self.config['byte_unit']:
                    if units:
                        value = diamond.convertor.binary.convert(value=value,
                                                                 oldUnit=units,
                                                                 newUnit=unit)
                    self.publish(name, value, metric_type='GAUGE')

                    # TODO: We only support one unit node here. Fix it!
                    break
            return True
        else:
            if not psutil:
                self.log.error('Unable to import psutil')
                self.log.error('No memory metrics retrieved')
                return None

            phymem_usage = psutil.phymem_usage()
            virtmem_usage = psutil.virtmem_usage()
            units = 'B'

            for unit in self.config['byte_unit']:
                value = diamond.convertor.binary.convert(
                    value=phymem_usage.total, oldUnit=units, newUnit=unit)
                self.publish('MemTotal', value, metric_type='GAUGE')

                value = diamond.convertor.binary.convert(
                    value=phymem_usage.free, oldUnit=units, newUnit=unit)
                self.publish('MemFree', value, metric_type='GAUGE')

                value = diamond.convertor.binary.convert(
                    value=phymem_usage.percent, oldUnit=units, newUnit=unit)
                self.publish('MemPercentUsed', value, metric_type='GAUGE')

                value = diamond.convertor.binary.convert(
                    value=virtmem_usage.total, oldUnit=units, newUnit=unit)
                self.publish('SwapTotal', value, metric_type='GAUGE')

                value = diamond.convertor.binary.convert(
                    value=virtmem_usage.free, oldUnit=units, newUnit=unit)
                self.publish('SwapFree', value, metric_type='GAUGE')

                # TODO: We only support one unit node here. Fix it!
                break

            return True

        return None
