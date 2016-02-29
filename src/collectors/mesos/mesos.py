# coding=utf-8

"""
Collect statistics from Mesos masters and slaves.

#### Dependencies

 * urllib2

#### Example Configuration

MesosCollector.conf

```
    enabled = True
    hosts = localhost:5050, localhost:5051, etc
```

Metrics are collected as:
    servers.<hostname>.mesos.<metric>

    Characters not in [A-Za-z0-9:-_] in metric names are replaced by _
"""

import json
from itertools import chain
import os
import re
import sys
import urllib2
import diamond.collector
import diamond.convertor
from diamond.collector import str_to_bool

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess32 as subprocess
else:
    import subprocess

# Metric Types
GAUGE = 'GAUGE'
COUNTER = 'COUNTER'

class Size(object):
    KB = 1024
    MB = KB * 1024

class Metric(object):
    def __init__(self, name, metric_type):
        self.name = name
        self.metric_type = metric_type

    def get_metrics(self):
        return {
            self.name: self.metric_type
        }

class MesosCollector(diamond.collector.Collector):

    # If this metric has a non-zero value, the instance is the active scheduler.
    ELECTED_MASTER_METRIC = 'master/elected'

    # As of Mesos 0.20.1
    MESOS_METRICS = [
        # Master
        Metric('master/cpus_percent', GAUGE),
        Metric('master/cpus_total', GAUGE),
        Metric('master/cpus_used', GAUGE),
        Metric('master/disk_percent', GAUGE),
        Metric('master/disk_total', GAUGE),
        Metric('master/disk_used', GAUGE),
        Metric('master/dropped_messages', COUNTER),
        Metric('master/elected', GAUGE),
        Metric('master/event_queue_dispatches', GAUGE),
        Metric('master/event_queue_http_requests', GAUGE),
        Metric('master/event_queue_messages', GAUGE),
        Metric('master/frameworks_active', GAUGE),
        Metric('master/frameworks_inactive', GAUGE),
        Metric('master/invalid_framework_to_executor_messages', COUNTER),
        Metric('master/invalid_status_update_acknowledgements', COUNTER),
        Metric('master/invalid_status_updates', COUNTER),
        Metric('master/mem_percent', GAUGE),
        Metric('master/mem_total', GAUGE),
        Metric('master/mem_used', GAUGE),
        Metric('master/messages_authenticate', COUNTER),
        Metric('master/messages_deactivate_framework', COUNTER),
        Metric('master/messages_exited_executor', COUNTER),
        Metric('master/messages_framework_to_executor', COUNTER),
        Metric('master/messages_kill_task', COUNTER),
        Metric('master/messages_launch_tasks', COUNTER),
        Metric('master/messages_reconcile_tasks', COUNTER),
        Metric('master/messages_register_framework', COUNTER),
        Metric('master/messages_register_slave', COUNTER),
        Metric('master/messages_reregister_framework', COUNTER),
        Metric('master/messages_reregister_slave', COUNTER),
        Metric('master/messages_resource_request', COUNTER),
        Metric('master/messages_revive_offers', COUNTER),
        Metric('master/messages_status_update', COUNTER),
        Metric('master/messages_status_update_acknowledgement', COUNTER),
        Metric('master/messages_unregister_framework', COUNTER),
        Metric('master/messages_unregister_slave', COUNTER),
        Metric('master/outstanding_offers', GAUGE),
        Metric('master/recovery_slave_removals', COUNTER),
        Metric('master/slave_registrations', COUNTER),
        Metric('master/slave_removals', COUNTER),
        Metric('master/slave_reregistrations', COUNTER),
        Metric('master/slaves_active', GAUGE),
        Metric('master/slaves_inactive', GAUGE),
        Metric('master/slaves_connected', GAUGE),
        Metric('master/slaves_disconnected', GAUGE),
        Metric('master/tasks_failed', COUNTER),
        Metric('master/tasks_finished', COUNTER),
        Metric('master/tasks_killed', COUNTER),
        Metric('master/tasks_lost', COUNTER),
        Metric('master/tasks_running', GAUGE),
        Metric('master/tasks_staging', GAUGE),
        Metric('master/tasks_starting', GAUGE),
        Metric('master/uptime_secs', GAUGE),
        Metric('master/valid_framework_to_executor_messages', COUNTER),
        Metric('master/valid_status_update_acknowledgements', COUNTER),
        Metric('master/valid_status_updates', COUNTER),
        # Slave
        Metric('slave/executors_registering', GAUGE),
        Metric('slave/executors_running', GAUGE),
        Metric('slave/executors_terminated', COUNTER),
        Metric('slave/executors_terminating', GAUGE),
        Metric('slave/frameworks_active', GAUGE),
        Metric('slave/invalid_framework_messages', COUNTER),
        Metric('slave/invalid_status_updates', COUNTER),
        Metric('slave/recovery_errors', COUNTER),
        Metric('slave/registered', GAUGE),
        Metric('slave/tasks_failed', COUNTER),
        Metric('slave/tasks_finished', COUNTER),
        Metric('slave/tasks_killed', COUNTER),
        Metric('slave/tasks_lost', COUNTER),
        Metric('slave/tasks_running', GAUGE),
        Metric('slave/tasks_staging', GAUGE),
        Metric('slave/tasks_starting', GAUGE),
        Metric('slave/uptime_secs', GAUGE),
        Metric('slave/valid_framework_messages', COUNTER),
        Metric('slave/valid_status_updates', COUNTER),
        # Registrar
        Metric('registrar/queued_operations', GAUGE),
        Metric('registrar/registry_size_bytes', GAUGE),
        Metric('registrar/state_fetch_ms', GAUGE),
        Metric('registrar/state_store_ms', GAUGE),
        Metric('registrar/state_store_ms/count', COUNTER),
        Metric('registrar/state_store_ms/max', GAUGE),
        Metric('registrar/state_store_ms/min', GAUGE),
        Metric('registrar/state_store_ms/p50', GAUGE),
        Metric('registrar/state_store_ms/p90', GAUGE),
        Metric('registrar/state_store_ms/p95', GAUGE),
        Metric('registrar/state_store_ms/p99', GAUGE),
        Metric('registrar/state_store_ms/p999', GAUGE),
        Metric('registrar/state_store_ms/p9999', GAUGE),
        # System Metrics
        Metric('system/cpus_total', GAUGE),
        Metric('system/load_15min', GAUGE),
        Metric('system/load_1min', GAUGE),
        Metric('system/load_5min', GAUGE),
        Metric('system/mem_free_bytes', GAUGE),
        Metric('system/mem_total_bytes', GAUGE),
    ]

    def __init__(self, config, handlers):
        """
        Setup collector by constructing the full mapping of name to metric type.
        """
        super(MesosCollector, self).__init__(config, handlers)
        published_metrics = dict()
        for metric in self.MESOS_METRICS:
            published_metrics.update(metric.get_metrics())
        self.published_metrics = published_metrics

        self.raw_stats_only = str_to_bool(self.config['raw_stats_only'])

        ci = self.config.get('cluster_identifiers')
        if isinstance(ci, basestring):
            ci = [ci]

        self.log.debug('Setting cluster identifiers: %s', ci)
        self.cluster_identifiers = ci

    def get_default_config_help(self):
        config_help = super(MesosCollector, self).get_default_config_help()
        config_help.update({
            'hosts': 'List of hosts, and ports to collect. Set a cluster by '
                     + ' prefixing the host:port with cluster@',
            'collect_disk_usage': 'If true, directly collect disk usage stats '
                                  + 'instead of relying on numbers from Mesos.'
            })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings.
        """
        config = super(MesosCollector, self).get_default_config()
        config.update({
            'hosts': ['localhost:5050'],
            'path': 'mesos',
            'collect_disk_usage': True,
            'raw_stats_only': False,
            'container_service': 'mesos.container',
            'master_service': 'mesos.master',
            'slave_service': 'mesos.slave',
            'cluster_identifiers': ['group', 'cluster'],
            # Default numeric output.
            'byte_unit': ['mb'],
            'subprocess_timeout': 15
            })
        return config

    def _format_identifier(self, identifier):
        if not isinstance(identifier, basestring):
            return identifier

        return re.sub('[^A-Za-z0-9:-_]', '_', identifier)

    def _get_hosts(self):
        """
        Returns a generator of (hostname, port) tuples.
        """
        hosts = self.config.get('hosts')

        if isinstance(hosts, basestring):
            hosts = [hosts]

        for host in hosts:
            matches = re.search('((.+)\@)?([^:]+)(:(\d+))?', host)
            hostname = matches.group(3)
            port = matches.group(5)

            yield hostname, port

    def _fetch_data(self, host, port, url):
        """
        Fetch data from an endpoint and return the parsed JSON result.
        """
        url = 'http://%s:%s/%s' % (host, port, url)
        headers = {'User-Agent': 'Diamond Mesos Collector', }

        self.log.debug('Requesting Mesos data from: %s' % url)
        req = urllib2.Request(url, headers=headers)

        handle = urllib2.urlopen(req)
        return json.loads(handle.read())

    def _publish_metrics(self, raw_name, raw_value, is_master):
        """
        Publishes explicitly exposed metrics.
        """
        # If the value can't be coerced to a float, don't publish.
        try:
            value = float(raw_value)
        except ValueError:
            return

        if is_master:
            service = self.config['master_service']
        else:
            service = self.config['slave_service']

        if self.raw_stats_only:
            self.publish(raw_name, value, service=service)
            return

        # If the name is in the known list, immediately return.
        metric_type = self.published_metrics.get(raw_name)
        if not metric_type:
            return

        name = self._format_identifier(raw_name)
        self.publish(name, value, metric_type=metric_type, service=service)

    def _merge_dicts(self, *args):
        result = {}
        for a in args:
            result.update(a)

        return result

    def _publish_task_metrics(self, cluster, host, port, state):
        slave_data = self._fetch_data(host, port, 'monitor/statistics.json')
        state_data = {
            i['id']: i for i in chain.from_iterable(
                f['executors'] for f in state['frameworks'])}

        for executor in slave_data:
            name = executor['executor_name']
            stats = executor['statistics']
            executor_state = state_data.get(executor['executor_id'])
            if name == 'aurora.gc':
                continue
            else:
                job_name = executor['source']
                instance_id = job_name.split('.')[-1]
                base_job_name = job_name[0:job_name.rindex('.')]
                source = '.'.join(filter(None, (cluster, base_job_name)))

            cpu = self._get_cpu_metrics(source, instance_id, stats)
            memory = self._get_memory_metrics(stats, state)
            disk = self._get_disk_metrics(stats, executor_state)

            metrics = self._merge_dicts(cpu, memory, disk)
            self._publish_metric_set(source, instance_id, **metrics)

    def _get_cpu_metrics(self, source, instance_id, stats):
        if self.raw_stats_only:
            return {
                k: v for k, v in stats.items()
                if k.startswith('cpu') and v is not None}
        else:
            # Calculate CPU delta
            user_cpu = self._calculate_derivative_metric(
                source,
                instance_id,
                stats,
                'cpus_user_time_secs',
                'user_cpu')
            sys_cpu = self._calculate_derivative_metric(
                source,
                instance_id,
                stats,
                'cpus_system_time_secs',
                'sys_cpu')
            if 'cpus_throttled_time_secs' in stats:
                time_throttled = self._calculate_derivative_metric(
                    source,
                    instance_id,
                    stats,
                    'cpus_throttled_time_secs',
                    'time_throttled')
            else:
                time_throttled = 0

            total_cpu = user_cpu + sys_cpu

            return {
                'user_cpu': user_cpu,
                'sys_cpu': sys_cpu,
                'cpu': total_cpu,
                'time_throttled': time_throttled,
            }

    @staticmethod
    def get_mesos_version(state):
        return tuple(int(part) for part in state['version'].split('.'))

    def _get_memory_metrics(self, stats, state):
        if self.get_mesos_version(state) >= (0, 23, 0):
            results = {}
            for k, v in stats.items():
                if v is None:
                    continue
                if not k.startswith('mem'):
                    continue
                if k.endswith('bytes') and not self.raw_stats_only:
                    for unit in self.config['byte_unit']:
                        key = re.sub('bytes$', unit, k)
                        value = diamond.convertor.binary.convert(
                            value=v, oldUnit='byte', newUnit=unit)
                        results[key] = value
                else:
                    results[k] = v
            return results
        else:
            # mem_rss_bytes (before mesos 0.23.0) is the total memory used
            # including the file cache, as reported from memory.usage_in_bytes.
            # anon_bytes is the RSS as reported from "total_rss" in memory.stat
            mem_total = stats['mem_rss_bytes'] / Size.MB
            mem_rss = stats['mem_anon_bytes'] / Size.MB

            # File cache memory isn't always available.
            mem_file = stats.get('mem_file_bytes')
            if mem_file is not None:
                mem_file /= Size.MB

            return {
                'mem_total': mem_total,
                'mem_file': mem_file,
                'mem_resident': mem_rss,
            }

    def _get_disk_metrics(self, stats, executor_state):
        """
        Gets disk usage statistics.
        """
        disk_limit = None
        disk_used = None
        if self.config.get('collect_disk_usage'):
            directory = executor_state.get('directory')
            # If there are no active tasks, limits from Mesos are not correct.
            tasks = executor_state.get('tasks')
            if directory and tasks:
                command = ['du', '-s', '-k', directory]
                try:
                    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
                    timeout = int(self.config['subprocess_timeout'])
                    out, _ = proc.communicate(timeout=timeout)

                    disk_used = int(out.split('\t')[0]) * Size.KB
                    disk_limit = executor_state['resources']['disk'] * Size.MB
                except Exception:
                    self.log.exception('Unable to collect disk usage.')
        else:
            disk_limit = stats.get('disk_limit_bytes')
            disk_used = stats.get('disk_used_bytes')

        results = {}
        if self.raw_stats_only:
            results['disk_limit_bytes'] = disk_limit or 0
            results['disk_used_bytes'] = disk_used or 0
        else:
            if disk_limit and disk_used:
                results['disk_percent'] = (float(disk_used) / float(disk_limit)) * 100

            if disk_used:
                for unit in self.config['byte_unit']:
                    results['disk_%s' % unit] = diamond.convertor.binary.convert(
                        value=disk_used, oldUnit='byte', newUnit=unit)

        return results

    def _publish_metric_set(self, source, instance_id, **metrics):
        """
        Publishes dictionary of metrics for a given source/instance.
        """
        full_source = "%s.%s" % (source, instance_id)
        for k, v in metrics.iteritems():
            if v is not None:
                if self.raw_stats_only:
                    name = k
                else:
                    name = 'job_resources_used_%s' % k
                service = self.config.get('container_service')
                self.publish(
                    name, v, source=full_source, service=service, groups=[source])

    def _calculate_derivative_metric(self, source, instance_id, stats,
                                     stat_name, metric_name):
        metric = '%s.%s.%s' % (metric_name, source, instance_id)
        return self.derivative(metric,
                               stats[stat_name],
                               timestamp=float(stats['timestamp']))

    def collect(self):
        """
        Runs collection processes against all available endpoints.
        """
        for host, port in self._get_hosts():
            try:
                metrics = self._fetch_data(host, port, 'metrics/snapshot')

                # If this is a master but not the elected master, don't publish.
                is_primary_master = metrics.get(self.ELECTED_MASTER_METRIC)
                if is_primary_master is not None and not is_primary_master:
                    self.log.warn('Non-elected master. Skipping collection.')
                    return

                # Publish explicitly exposed metrics.
                for raw_name, raw_value in metrics.iteritems():
                    self._publish_metrics(
                        raw_name, raw_value, is_master=is_primary_master)

                # IMPORTANT: do not fetch state.json from the master; it is a
                # blocking operation that interferes with basic Mesos
                # operations.
                if is_primary_master is None:
                    state = self._fetch_data(host, port, 'state.json')
                    a = state['attributes']
                    cluster = None
                    for ci in self.cluster_identifiers:
                        cluster = a.get(ci)
                        if cluster:
                            break

                    # Publish task level metrics
                    self._publish_task_metrics(cluster, host, port, state)
            except Exception:
                self.log.exception(
                    "Error retrieving Mesos metrics for (%s, %s).", host, port)
