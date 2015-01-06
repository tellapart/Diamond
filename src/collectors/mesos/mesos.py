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

from collections import defaultdict
import itertools
import json
import re
import urllib2
import diamond.collector

# Metric Types
GAUGE = 'GAUGE'
COUNTER = 'COUNTER'

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
        published_metrics = dict()
        for metric in self.MESOS_METRICS:
            published_metrics.update(metric.get_metrics())
        self.published_metrics = published_metrics

        super(MesosCollector, self).__init__(config, handlers)

    def get_default_config_help(self):
        config_help = super(MesosCollector, self).get_default_config_help()
        config_help.update({
            'hosts': 'List of hosts, and ports to collect. Set a cluster by '
                     + ' prefixing the host:port with cluster@',
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
            #cluster = matches.group(2)
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

    def _publish_metrics(self, raw_name, raw_value):
        """
        Publishes explicitly exposed metrics.
        """
        # If the name is in the known list, immediately return.
        metric_type = self.published_metrics.get(raw_name)
        if not metric_type:
            return

        # If the value can't be coerced to a float, don't publish.
        try:
            value = float(raw_value)
        except ValueError:
            self.log.warn('Skipping %s due to non-numeric value %s',
                          raw_name, raw_value)
            return

        name = self._format_identifier(raw_name)
        self.publish(name, value, metric_type=metric_type)

    def collect(self):
        """
        Runs collection processes against all available endpoints.
        """
        for host, port in self._get_hosts():
            try:
                state = self._fetch_data(host, port, 'state.json')
                cluster = state['attributes'].get('group') or 'unknown'

                # TODO(george): If we want to publish stats using the actual
                # mesos cluster name, we can cut over to use state.json.
                # Unfortunately, it only returns the cluster name on the master.
                metrics = self._fetch_data(host, port, 'metrics/snapshot')

                # If this is a master but not the elected master, don't publish.
                is_master = metrics.get(self.ELECTED_MASTER_METRIC)
                if is_master is not None and not is_master:
                    self.log.warn('Non-elected master. Skipping collection.')
                    return

                # Publish explicitly exposed metrics.
                for raw_name, raw_value in metrics.iteritems():
                    self._publish_metrics(raw_name, raw_value)

                # Publish task level metrics
                self._publish_task_metrics(cluster, host, port)
            except Exception:
                self.log.exception(
                    "Error retrieving Mesos metrics for (%s, %s).", host, port)

    def _publish_task_metrics(self, cluster, host, port):
        slave_data = self._fetch_data(host, port, 'monitor/statistics.json')
        for executor in slave_data:
            name = executor['executor_name']
            stats = executor['statistics']
            if name == 'aurora.gc':
                continue
            else:
                job_name = executor['source']
                instance_id = job_name[job_name.rindex('.') + 1:]
                base_job_name = job_name[0:job_name.rindex('.')]
                source = cluster + '.' + base_job_name

            # Calculate CPU delta
            user_cpu = self._calculate_derivative_metric(source,
                                                         instance_id,
                                                         stats,
                                                         'cpus_user_time_secs',
                                                         'user_cpu')
            sys_cpu = self._calculate_derivative_metric(source,
                                                        instance_id,
                                                        stats,
                                                        'cpus_system_time_secs',
                                                        'sys_cpu')
            total_cpu = user_cpu + sys_cpu
            mem_used = stats['mem_rss_bytes'] / (1024 * 1024)
            self._public_metric_set(user_cpu, sys_cpu, total_cpu, mem_used, instance_id, source)

    def _public_metric_set(self, user_cpu, sys_cpu, total_cpu, mem_used, instance_id, source):
        instance_id = '.' + instance_id or '.total'
        self.publish('job_resources_used_user_cpu', user_cpu, source=source + instance_id)
        self.publish('job_resources_used_sys_cpu', sys_cpu, source=source + instance_id)
        self.publish('job_resources_used_cpu', total_cpu, source=source + instance_id)
        self.publish('job_resources_used_mem_reserved', mem_used, source=source + instance_id)

    def _calculate_derivative_metric(self, source, instance_id, stats,
                                     stat_name, metric_name):
        metric = metric_name + '.' + source + '.' + instance_id
        return self.derivative(metric,
                               stats[stat_name],
                               timestamp=float(stats['timestamp']))
