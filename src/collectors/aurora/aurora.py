# coding=utf-8

"""
Collect statistics from the Aurora scheduler.

#### Dependencies

 * urllib2

#### Example Configuration

AuroraCollector.conf

```
    enabled = True
    hosts = general@localhost:8081, www.example.com:8081, etc
    raw_stats_only = If True, only publishes numeric metrics from /vars.json.
```

Only supply a cluster name (e.g. general) if you intend to override the
self-declared cluster name.

Metrics are collected as:
    servers.<hostname>.aurora.<metric>

    Characters not in [A-Za-z0-9:-_] in metric names and source components are
    replaced by _
"""

import itertools
import json
import re
import urllib2
import diamond.collector
from diamond.collector import str_to_bool

# Metric Types
GAUGE = 'GAUGE'
COUNTER = 'COUNTER'
SLIDING = 'SLIDING'

class Metric(object):
    def __init__(self, name, metric_type):
        self.name = name
        self.metric_type = metric_type

    def get_metrics(self):
        return {
            self.name: self.metric_type
        }

class SlidingMetric(Metric):
    def __init__(self, name, units='nanos'):
        self.units = units
        super(SlidingMetric, self).__init__(name, SLIDING)

    def get_metrics(self):
        return {
            self.name + '_events': COUNTER,
            self.name + '_events_per_sec': GAUGE,
            self.name + '_failures': COUNTER,
            self.name + '_%s_per_event' % self.units: GAUGE,
            self.name + '_%s_total' % self.units: COUNTER,
            self.name + '_%s_total_per_sec' % self.units: GAUGE,
            self.name + '_timeouts': COUNTER,
        }

class RedirectError(Exception):
    pass

class NoRedirectHandler(urllib2.HTTPRedirectHandler):
    """Custom handler that fast-fails on redirect.
    """
    def http_error_302(self, req, fp, code, msg, headers):
        raise RedirectError()

    http_error_301 = http_error_303 = http_error_307 = http_error_302

class AuroraCollector(diamond.collector.Collector):

    # If this metric has a non-zero value, the instance is the active scheduler.
    ACTIVE_SCHEDULER_METRIC = 'scheduler_lifecycle_ACTIVE'

    # As of Aurora 0.6.0-incubating
    AURORA_METRICS = [
        Metric('async_tasks_completed', COUNTER),
        SlidingMetric('attribute_store_fetch_all'),
        SlidingMetric('attribute_store_fetch_one'),
        SlidingMetric('attribute_store_save'),
        Metric('cron_job_launch_failures', COUNTER),
        Metric('cron_jobs_loaded', COUNTER),
        Metric('empty_slots_dedicated_large', GAUGE),
        Metric('empty_slots_dedicated_medium', GAUGE),
        Metric('empty_slots_dedicated_small', GAUGE),
        Metric('empty_slots_dedicated_xlarge', GAUGE),
        Metric('empty_slots_large', GAUGE),
        Metric('empty_slots_medium', GAUGE),
        Metric('empty_slots_small', GAUGE),
        Metric('empty_slots_xlarge', GAUGE),
        Metric('framework_registered', GAUGE),
        Metric('gc_executor_tasks_lost', COUNTER),
        SlidingMetric('http_200_responses'),
        SlidingMetric('http_404_responses'),
        SlidingMetric('http_500_responses'),
        Metric('job_update_delete_errors', COUNTER),
        Metric('job_update_recovery_errors', COUNTER),
        Metric('job_update_state_change_errors', COUNTER),
        SlidingMetric('job_update_store_delete_all'),
        SlidingMetric('job_update_store_fetch_all_details'),
        SlidingMetric('job_update_store_fetch_summaries'),
        SlidingMetric('job_update_store_prune_history'),
        SlidingMetric('job_update_store_save_event'),
        SlidingMetric('job_update_store_save_instance_event'),
        SlidingMetric('job_update_store_save_update'),
        Metric('jvm_available_processors', GAUGE),
        Metric('jvm_class_loaded_count', COUNTER),
        Metric('jvm_class_total_loaded_count', COUNTER),
        Metric('jvm_class_unloaded_count', COUNTER),
        Metric('jvm_gc_Copy_collection_count', GAUGE),
        Metric('jvm_gc_Copy_collection_time_ms', GAUGE),
        Metric('jvm_gc_MarkSweepCompact_collection_count', GAUGE),
        Metric('jvm_gc_MarkSweepCompact_collection_time_ms', GAUGE),
        Metric('jvm_gc_collection_count', COUNTER),
        Metric('jvm_gc_collection_time_ms', GAUGE),
        Metric('jvm_memory_free_mb', GAUGE),
        Metric('jvm_memory_heap_mb_committed', GAUGE),
        Metric('jvm_memory_heap_mb_max', GAUGE),
        Metric('jvm_memory_heap_mb_used', GAUGE),
        Metric('jvm_memory_max_mb', GAUGE),
        Metric('jvm_memory_mb_total', GAUGE),
        Metric('jvm_memory_non_heap_mb_committed', GAUGE),
        Metric('jvm_memory_non_heap_mb_max', GAUGE),
        Metric('jvm_memory_non_heap_mb_used', GAUGE),
        Metric('jvm_threads_active', GAUGE),
        Metric('jvm_threads_daemon', GAUGE),
        Metric('jvm_threads_peak', GAUGE),
        Metric('jvm_threads_started', COUNTER),
        Metric('jvm_time_ms', COUNTER),
        Metric('jvm_uptime_secs', GAUGE),
        SlidingMetric('lock_store_delete_locks'),
        SlidingMetric('lock_store_fetch_lock'),
        SlidingMetric('lock_store_fetch_locks'),
        SlidingMetric('log_entry_serialize'),
        SlidingMetric('log_manager_append'),
        SlidingMetric('log_manager_deflate'),
        SlidingMetric('log_manager_snapshot'),
        SlidingMetric('mem_storage_consistent_read_operation'),
        SlidingMetric('mem_storage_delete_all_tasks'),
        SlidingMetric('mem_storage_delete_tasks'),
        SlidingMetric('mem_storage_fetch_tasks'),
        SlidingMetric('mem_storage_mutate_tasks'),
        SlidingMetric('mem_storage_save_tasks'),
        SlidingMetric('mem_storage_weakly_consistent_read_operation'),
        SlidingMetric('mem_storage_write_operation'),
        Metric('offer_accept_races', COUNTER),
        Metric('outstanding_offers', GAUGE),
        Metric('preemptor_attempts', COUNTER),
        Metric('preemptor_missing_attributes', COUNTER),
        Metric('preemptor_no_slots_found', COUNTER),
        Metric('preemptor_tasks_preempted', COUNTER),
        Metric('process_cpu_cores_utilized', GAUGE),
        Metric('process_cpu_time_nanos', COUNTER),
        Metric('process_max_fd_count', GAUGE),
        Metric('process_open_fd_count', GAUGE),
        Metric('pubsub_executor_queue_size', GAUGE),
        Metric('quartz_scheduler_running', GAUGE),
        SlidingMetric('quota_store_delete_quotas'),
        SlidingMetric('quota_store_fetch_quota'),
        SlidingMetric('quota_store_fetch_quotas'),
        SlidingMetric('quota_store_save_quota'),
        Metric('reservation_cache_size', GAUGE),
        Metric('resources_allocated_quota_cpu', GAUGE),
        Metric('resources_allocated_quota_disk_gb', GAUGE),
        Metric('resources_allocated_quota_ram_gb', GAUGE),
        Metric('resources_dedicated_consumed_cpu', GAUGE),
        Metric('resources_dedicated_consumed_disk_gb', GAUGE),
        Metric('resources_dedicated_consumed_ram_gb', GAUGE),
        Metric('resources_free_pool_consumed_cpu', GAUGE),
        Metric('resources_free_pool_consumed_disk_gb', GAUGE),
        Metric('resources_free_pool_consumed_ram_gb', GAUGE),
        Metric('resources_quota_consumed_cpu', GAUGE),
        Metric('resources_quota_consumed_disk_gb', GAUGE),
        Metric('resources_quota_consumed_ram_gb', GAUGE),
        Metric('resources_total_consumed_cpu', GAUGE),
        Metric('resources_total_consumed_disk_gb', GAUGE),
        Metric('resources_total_consumed_ram_gb', GAUGE),
        Metric('schedule_attempts_failed', COUNTER),
        Metric('schedule_attempts_fired', COUNTER),
        Metric('schedule_attempts_no_match', COUNTER),
        Metric('schedule_queue_size', GAUGE),
        SlidingMetric('scheduled_task_penalty', 'ms'),
        Metric('scheduler_backup_failed', COUNTER),
        Metric('scheduler_backup_success', COUNTER),
        Metric('scheduler_driver_kill_failures', COUNTER),
        Metric('scheduler_framework_disconnects', COUNTER),
        Metric('scheduler_framework_reregisters', COUNTER),
        Metric('scheduler_gc_insufficient_offers', COUNTER),
        Metric('scheduler_gc_offers_consumed', COUNTER),
        Metric('scheduler_gc_tasks_created', COUNTER),
        Metric('scheduler_illegal_task_state_transitions', COUNTER),
        Metric('scheduler_lifecycle_ACTIVE', GAUGE),
        Metric('scheduler_lifecycle_DEAD', GAUGE),
        Metric('scheduler_lifecycle_IDLE', GAUGE),
        Metric('scheduler_lifecycle_LEADER_AWAITING_REGISTRATION', GAUGE),
        Metric('scheduler_lifecycle_PREPARING_STORAGE', GAUGE),
        Metric('scheduler_lifecycle_STORAGE_PREPARED', GAUGE),
        Metric('scheduler_log_bad_frames_read', COUNTER),
        Metric('scheduler_log_bytes_read', COUNTER),
        Metric('scheduler_log_bytes_written', COUNTER),
        Metric('scheduler_log_deflated_entries_read', COUNTER),
        Metric('scheduler_log_entries_read', COUNTER),
        Metric('scheduler_log_entries_written', COUNTER),
        SlidingMetric('scheduler_log_native_append'),
        Metric('scheduler_log_native_native_entries_skipped', COUNTER),
        SlidingMetric('scheduler_log_native_read'),
        SlidingMetric('scheduler_log_native_truncate'),
        SlidingMetric('scheduler_log_recover'),
        SlidingMetric('scheduler_log_snapshot'),
        SlidingMetric('scheduler_log_snapshot_persist'),
        Metric('scheduler_log_snapshots', COUNTER),
        Metric('scheduler_log_un_snapshotted_transactions', GAUGE),
        Metric('scheduler_lost_executors', COUNTER),
        Metric('scheduler_resource_offers', COUNTER),
        SlidingMetric('scheduler_resource_offers'),
        SlidingMetric('scheduler_status_update'),
        Metric('scheduler_status_updates', COUNTER),
        SlidingMetric('scheduler_store_fetch_framework_id'),
        SlidingMetric('scheduler_store_save_framework_id'),
        SlidingMetric('scheduler_thrift_createJob'),
        SlidingMetric('scheduler_thrift_finalize'),
        SlidingMetric('scheduler_thrift_getConfigSummary'),
        SlidingMetric('scheduler_thrift_getJobUpdateSummaries'),
        SlidingMetric('scheduler_thrift_getPendingReason'),
        SlidingMetric('scheduler_thrift_getQuota'),
        SlidingMetric('scheduler_thrift_getRoleSummary'),
        SlidingMetric('scheduler_thrift_getTasksWithoutConfigs'),
        SlidingMetric('scheduler_thrift_getVersion'),
        SlidingMetric('scheduler_thrift_setQuota'),
        Metric('shard_sanity_check_failures', COUNTER),
        SlidingMetric('sla_stats_computation'),
        SlidingMetric('snapshot_apply'),
        SlidingMetric('snapshot_create'),
        Metric('storage_lock_threads_waiting', GAUGE),
        Metric('task_config_keys_backfilled', COUNTER),
        Metric('task_kill_retries', COUNTER),
        Metric('task_queries_all', COUNTER),
        Metric('task_queries_by_host', COUNTER),
        Metric('task_queries_by_id', COUNTER),
        Metric('task_queries_by_job', COUNTER),
        SlidingMetric('task_schedule_attempt'),
        Metric('task_store_ASSIGNED', GAUGE),
        Metric('task_store_DRAINING', GAUGE),
        Metric('task_store_FAILED', GAUGE),
        Metric('task_store_FINISHED', GAUGE),
        Metric('task_store_INIT', GAUGE),
        Metric('task_store_KILLED', GAUGE),
        Metric('task_store_KILLING', GAUGE),
        Metric('task_store_LOST', GAUGE),
        Metric('task_store_PENDING', GAUGE),
        Metric('task_store_PREEMPTING', GAUGE),
        Metric('task_store_RESTARTING', GAUGE),
        Metric('task_store_RUNNING', GAUGE),
        Metric('task_store_SANDBOX_DELETED', GAUGE),
        Metric('task_store_STARTING', GAUGE),
        Metric('task_store_THROTTLED', GAUGE),
        SlidingMetric('task_throttle', 'ms'),
        Metric('timed_out_tasks', COUNTER),
        Metric('timeout_queue_size', GAUGE),
        Metric('uncaught_exceptions', COUNTER),
        SlidingMetric('variable_scrape', 'micros'),
    ] + [  # SLA statistics - see sla.md in the Aurora source for more detail.
        Metric('sla_%s_%s_%s_ms' % (t, s, m), GAUGE) for t, s, m in
        itertools.product(
            ('cpu', 'ram', 'disk'),
            ('small', 'medium', 'large', 'xlarge', 'xxlarge'),
            ('mttr', 'mtta', 'mtta_nonprod', 'mttr_nonprod')
        )
    ] + [
        Metric('sla_cluster_platform_uptime_percent', GAUGE),
        Metric('sla_cluster_mtta_ms', GAUGE),
        Metric('sla_cluster_mtta_nonprod_ms', GAUGE),
        Metric('sla_cluster_mttr_ms', GAUGE),
        Metric('sla_cluster_mttr_nonprod_ms', GAUGE),
    ]

    def __init__(self, config, handlers):
        """
        Setup collector by constructing the full mapping of name to metric type.
        """
        super(AuroraCollector, self).__init__(config, handlers)
        published_metrics = dict()
        for metric in self.AURORA_METRICS:
            published_metrics.update(metric.get_metrics())
        self.published_metrics = published_metrics

        self.dynamic_metrics = [
            ('sla_job_%s', GAUGE, self._generate_sla_regex()),
            ('job_stats_%s_tasks', COUNTER, self._generate_tasks_regex())
        ]

        self.raw_stats_only = str_to_bool(self.config['raw_stats_only'])
        self.collect_quota = str_to_bool(self.config['collect_quota'])
        self.http_timeout = int(self.config.get('http_timeout'))

    def get_default_config_help(self):
        config_help = super(AuroraCollector, self).get_default_config_help()
        config_help.update({
            'hosts': 'List of hosts, and ports to collect. Set a cluster by '
                     + ' prefixing the host:port with cluster@',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings.
        """
        config = super(AuroraCollector, self).get_default_config()
        config.update({
            'hosts': ['localhost:8081'],
            'path': 'aurora',
            'thermos_executor_cpu_overhead': 0.25,
            'thermos_executor_mem_overhead': 128,
            'raw_stats_only': False,
            'collect_quota': True,
            'quota_service': 'aurora.quota',
            'quota_group': 'aurora.quota',
            'quota_prefix': 'sd',
            'http_timeout': 30
        })
        return config

    def _generate_tasks_regex(self):
        """
        Generate a regular expression for capturing dynamic SLA gauges.
        """
        task_metrics = ['FAILED', 'LOST']
        re_str = '^tasks_(?P<metric>%s)_(?P<role>.+)/(?P<env>.+)/(?P<job>.+)$'
        return re.compile(re_str % '|'.join(task_metrics))

    def _generate_sla_regex(self):
        """
        Generate a regular expression for capturing dynamic SLA gauges.
        """
        sla_metrics = [
            'mttr_nonprod_ms',
            'mtta_nonprod_ms',
            'mttr_ms',
            'mtta_ms',
            'platform_uptime_percent'
        ]
        sla_metrics += [
            'job_uptime_%d.00_sec' % u for u in [50, 75, 90, 95, 99]
        ]
        re_str = '^sla_(?P<role>.+)/(?P<env>.+)/(?P<job>.+)_(?P<metric>%s)$'
        return re.compile(re_str % '|'.join(sla_metrics))

    def _extract_metric(self, raw_name, cluster):
        """
        Extract name/type
        """
        # If the name is in the known list, immediately return.
        metric_type = self.published_metrics.get(raw_name)
        if metric_type:
            return raw_name, metric_type, None

        # If we have a hit on a regex, extract the name + source from raw_name.
        for format_str, metric_type, regex in self.dynamic_metrics:
            match = regex.match(raw_name)
            if match:
                role = match.group('role')
                env = match.group('env')
                job = match.group('job')
                metric = match.group('metric')

                name = format_str % metric.lower()
                source = self._create_source(cluster, role, env, job)
                return name, metric_type, source

        return None, None, None

    def _create_source(self, *parts):
        return '%s.%s.%s.%s' % tuple(
            map(self._format_identifier, parts)
        )

    def _format_identifier(self, identifier):
        if not isinstance(identifier, basestring):
            return identifier

        return re.sub('[^A-Za-z0-9:-_]', '_', identifier)

    def _get_hosts(self):
        """
        Returns a generator of (cluster, hostname, port) tuples.
        """
        hosts = self.config.get('hosts')

        if isinstance(hosts, basestring):
            hosts = [hosts]

        for host in hosts:
            matches = re.search('((.+)\@)?([^:]+)(:(\d+))?', host)
            cluster = matches.group(2)
            hostname = matches.group(3)
            port = matches.group(5)

            yield cluster, hostname, port

    def _fetch_data(self, host, port, url, verb='GET', data=None):
        """
        Fetch data from an endpoint and return the parsed JSON result.
        """
        url = 'http://%s:%s/%s' % (host, port, url)
        headers = {
            'User-Agent': 'Diamond Aurora Scheduler Collector',
            'Content-Type': 'application/json'
        }

        self.log.debug('Requesting Aurora data from: %s' % url)
        req = urllib2.Request(url, headers=headers,
                              data=None if verb == 'GET' else data or '')
        opener = urllib2.build_opener(NoRedirectHandler)
        handle = opener.open(req, timeout=self.http_timeout)
        return json.loads(handle.read())

    def _publish_job_metrics(self, job_summary, cluster):
        """
        Publishes metrics derived from a job summary.
        """
        key = job_summary['job']['key']
        source = self._create_source(
            cluster, key['role'], key['environment'], key['name'])

        stats = job_summary['stats']
        tasks = [
            ('active', float(stats['activeTaskCount']), GAUGE),
            ('pending', float(stats['pendingTaskCount']), GAUGE),
            # Do not collect from job summary API; the counters can go backwards
            # as older tasks are cleaned up.
            #('finished', float(stats['finishedTaskCount']), COUNTER),
            #('failed', float(stats['failedTaskCount']), COUNTER)
        ]

        for task, value, metric_type in tasks:
            self.publish('job_stats_%s_tasks' % task, value,
                         metric_type=metric_type, source=source)

        task_config = job_summary['job']['taskConfig']
        total_tasks = (float(stats['activeTaskCount']) +
                       float(stats['pendingTaskCount']))

        cpu_overhead = self.config.get('thermos_executor_cpu_overhead')
        mem_overhead = self.config.get('thermos_executor_mem_overhead')
        resources = [
            ('cpu', float(task_config['numCpus']), float(cpu_overhead)),
            ('ram_mb', float(task_config['ramMb']), float(mem_overhead)),
            ('disk_mb', float(task_config['diskMb']), 0),
        ]

        for resource, value, overhead in resources:
            self.publish('job_resources_total_allocated_%s' % resource,
                         value * total_tasks, metric_type=GAUGE, source=source)
            if overhead != 0:
                self.publish(
                    'job_resources_total_allocated_%s_adjusted' % resource,
                    (value + overhead) * total_tasks, metric_type=GAUGE,
                    source=source)

    def _publish_metrics(self, raw_name, raw_value, cluster):
        """
        Publishes explicitly exposed metrics.
        """
        # If the value can't be coerced to a float, don't publish.
        try:
            value = float(raw_value)
        except ValueError:
            return

        if self.raw_stats_only:
            self.publish(raw_name, value)
            return

        name, metric_type, source = self._extract_metric(raw_name, cluster)
        if not name:
            return

        name = self._format_identifier(name)
        self.publish(name, value, metric_type=metric_type, source=source)

    def _publish_quota_metrics(self, raw_data):
        """
        Publishes quota information per-role.
        """
        prefix = self.config.get('quota_prefix')
        for role, values in raw_data.items():
            source = '.'.join(filter(None, (prefix, role)))
            for name, value in values.items():
                self.publish(
                    name,
                    value,
                    source=source,
                    groups=[self.config.get('quota_group')],
                    service=self.config.get('quota_service'))

    def collect(self):
        """
        Runs collection processes against all available endpoints.
        """
        for cluster, host, port in self._get_hosts():
            try:
                try:
                    role_summary = self._fetch_data(
                        host, port, 'apibeta/getRoleSummary', verb='POST')
                except RedirectError:
                    self.log.warn(
                        'GetRoleSummmary returned a 30X code, which indicates '
                        'an inactive scheduler. Skipping collection.')
                    return

                metrics = self._fetch_data(host, port, 'vars.json')
                cluster = cluster or role_summary['serverInfo']['clusterName']

                # If this is not the active scheduler, don't publish metrics.
                if not metrics.get(self.ACTIVE_SCHEDULER_METRIC):
                    self.log.warn('Inactive scheduler. Skipping collection.')
                    return

                if not self.raw_stats_only:
                    # Publish metrics contained in the job summary information.
                    roles = role_summary['result']['roleSummaryResult']['summaries']
                    for r in roles:
                        body = json.dumps({'role': r['role']})
                        res = self._fetch_data(
                            host, port, 'apibeta/getJobSummary', verb='POST',
                            data=body)
                        summaries = res['result']['jobSummaryResult']['summaries']
                        for js in summaries:
                            self._publish_job_metrics(js, cluster)

                # Publish explicitly exposed metrics.
                for raw_name, raw_value in metrics.iteritems():
                    self._publish_metrics(raw_name, raw_value, cluster)

                if self.collect_quota:
                    quota_data = self._fetch_data(host, port, 'quotas')
                    self._publish_quota_metrics(quota_data)

            except Exception:
                self.log.exception(
                    "Error retrieving Aurora metrics for (%s, %s, %s).",
                    cluster, host, port)
