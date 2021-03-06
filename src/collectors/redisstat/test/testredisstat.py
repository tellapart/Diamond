#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from test import run_only
from mock import Mock
from mock import patch, call

from diamond.collector import Collector
from redisstat import RedisCollector

################################################################################


def run_only_if_redis_is_available(func):
    """Decorator for checking if python-redis is available.
    Note: this test will be silently skipped if python-redis is missing.
    """
    try:
        import redis
        redis  # workaround for pyflakes issue #13
    except ImportError:
        redis = None
    pred = lambda: redis is not None
    return run_only(func, pred)


class TestRedisCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('RedisCollector', {
            'interval': '1',
            'databases': 1,
        })

        self.collector = RedisCollector(config, None)

    def test_import(self):
        self.assertTrue(RedisCollector)

    @run_only_if_redis_is_available
    @patch.object(Collector, 'publish')
    def test_real_data(self, publish_mock):

        data_1 = {'pubsub_channels': 0,
                  'used_memory_peak_human': '700.71K',
                  'bgrewriteaof_in_progress': 0,
                  'connected_slaves': 0,
                  'uptime_in_days': 0,
                  'multiplexing_api': 'epoll',
                  'lru_clock': 954113,
                  'last_save_time': 1351718385,
                  'redis_version': '2.4.10',
                  'redis_git_sha1': 0,
                  'gcc_version': '4.4.6',
                  'connected_clients': 1,
                  'keyspace_misses': 0,
                  'used_memory': 726144,
                  'vm_enabled': 0,
                  'used_cpu_user_children': '0.00',
                  'used_memory_peak': 717528,
                  'role': 'master',
                  'total_commands_processed': 1,
                  'latest_fork_usec': 0,
                  'loading': 0,
                  'used_memory_rss': 7254016,
                  'total_connections_received': 1,
                  'pubsub_patterns': 0,
                  'aof_enabled': 0,
                  'used_cpu_sys': '0.02',
                  'used_memory_human': '709.12K',
                  'used_cpu_sys_children': '0.00',
                  'blocked_clients': 0,
                  'used_cpu_user': '0.00',
                  'client_biggest_input_buf': 0,
                  'arch_bits': 64,
                  'mem_fragmentation_ratio': '9.99',
                  'expired_keys': 0,
                  'evicted_keys': 0,
                  'bgsave_in_progress': 0,
                  'client_longest_output_list': 0,
                  'mem_allocator': 'jemalloc-2.2.5',
                  'process_id': 3020,
                  'uptime_in_seconds': 32,
                  'changes_since_last_save': 0,
                  'redis_git_dirty': 0,
                  'keyspace_hits': 0,
                  'rejected_connections': 10,
                  'cmdstat_cluster': {
                      'usec_per_call': 101,
                      'calls': 300,
                      'usec': 20
                  }}
        data_2 = {'pubsub_channels': 1,
                  'used_memory_peak_human': '1700.71K',
                  'bgrewriteaof_in_progress': 4,
                  'connected_slaves': 2,
                  'uptime_in_days': 1,
                  'multiplexing_api': 'epoll',
                  'lru_clock': 5954113,
                  'last_save_time': 51351718385,
                  'redis_version': '2.4.10',
                  'redis_git_sha1': 0,
                  'gcc_version': '4.4.6',
                  'connected_clients': 100,
                  'keyspace_misses': 670,
                  'used_memory': 1726144,
                  'vm_enabled': 0,
                  'used_cpu_user_children': '2.00',
                  'used_memory_peak': 1717528,
                  'role': 'master',
                  'total_commands_processed': 19764,
                  'latest_fork_usec': 8,
                  'loading': 0,
                  'used_memory_rss': 17254016,
                  'total_connections_received': 18764,
                  'pubsub_patterns': 0,
                  'aof_enabled': 0,
                  'used_cpu_sys': '0.05',
                  'used_memory_human': '1709.12K',
                  'used_cpu_sys_children': '0.09',
                  'blocked_clients': 8,
                  'used_cpu_user': '0.09',
                  'client_biggest_input_buf': 40,
                  'arch_bits': 64,
                  'mem_fragmentation_ratio': '0.99',
                  'expired_keys': 0,
                  'evicted_keys': 0,
                  'bgsave_in_progress': 0,
                  'client_longest_output_list': 0,
                  'mem_allocator': 'jemalloc-2.2.5',
                  'process_id': 3020,
                  'uptime_in_seconds': 95732,
                  'changes_since_last_save': 759,
                  'redis_git_dirty': 0,
                  'keyspace_hits': 5700,
                  'rejected_connections': 25,
                  'cmdstat_cluster': {
                      'usec_per_call': 101,
                      'calls': 378,
                      'usec': 120,
                      'p250': 81,
                      'p500': 91,
                      'p900': 122,
                      'p990': 191,
                      'p999': 253
                  }}

        patch_collector = patch.object(RedisCollector, '_get_info',
                                       Mock(return_value=data_1))
        patch_time = patch('time.time', Mock(return_value=10))

        patch_collector.start()
        patch_time.start()
        self.collector.collect()
        patch_collector.stop()
        patch_time.stop()

        self.assertPublishedMany(publish_mock, {})

        patch_collector = patch.object(RedisCollector, '_get_info',
                                       Mock(return_value=data_2))
        patch_time = patch('time.time', Mock(return_value=20))

        patch_collector.start()
        patch_time.start()
        self.collector.collect()
        patch_collector.stop()
        patch_time.stop()

        metrics = {'process.uptime': 95732,
                   'pubsub.channels': 1,
                   'slaves.connected': 2,
                   'process.connections_received': 18764,
                   'clients.longest_output_list': 0,
                   'process.commands_processed': 19764,
                   'last_save.changes_since': 759,
                   'memory.used_memory_rss': 17254016,
                   'memory.fragmentation_ratio': 0.99,
                   'last_save.time': 51351718385,
                   'clients.connected': 100,
                   'clients.blocked': 8,
                   'pubsub.patterns': 0,
                   'cpu.parent.user': 0.09,
                   'last_save.time_since': -51351718365,
                   'memory.used_memory': 1726144,
                   'cpu.parent.sys': 0.05,
                   'keyspace.misses': 670,
                   'keys.expired': 0,
                   'keys.evicted': 0,
                   'keyspace.hits': 5700,
                   'clients.rejected_connections': 15,
                   'cmdstat.cluster.usec_per_call': 101,
                   'cmdstat.cluster.usec_per_call_p250': 81,
                   'cmdstat.cluster.usec_per_call_p500': 91,
                   'cmdstat.cluster.usec_per_call_p900': 122,
                   'cmdstat.cluster.usec_per_call_p990': 191,
                   'cmdstat.cluster.usec_per_call_p999': 253,
                   'cmdstat.cluster.calls': 78,
                   'cmdstat.cluster.usec': 100
                   }

        self.assertPublishedMany(publish_mock, metrics)

        self.setDocExample(collector=self.collector.__class__.__name__,
                           metrics=metrics,
                           defaultpath=self.collector.config['path'])

    @run_only_if_redis_is_available
    @patch.object(Collector, 'publish')
    def test_hostport_or_instance_config(self, publish_mock):

        testcases = {
            'default': {
                'config': {},  # test default settings
                'calls': [call('localhost', 6379, None)],
            },
            'host_set': {
                'config': {'host': 'myhost'},
                'calls': [call('myhost', 6379, None)],
            },
            'port_set': {
                'config': {'port': 5005},
                'calls': [call('localhost', 5005, None)],
            },
            'hostport_set': {
                'config': {'host': 'megahost', 'port': 5005},
                'calls': [call('megahost', 5005, None)],
            },
            'instance_1_host': {
                'config': {'instances': ['nick@myhost']},
                'calls': [call('myhost', 6379, None)],
            },
            'instance_1_port': {
                'config': {'instances': ['nick@:9191']},
                'calls': [call('localhost', 9191, None)],
            },
            'instance_1_hostport': {
                'config': {'instances': ['nick@host1:8765']},
                'calls': [call('host1', 8765, None)],
            },
            'instance_2': {
                'config': {'instances': ['foo@hostX', 'bar@:1000']},
                'calls': [
                    call('hostX', 6379, None),
                    call('localhost', 1000, None)
                ],
            },
            'old_and_new': {
                'config': {
                    'host': 'myhost',
                    'port': 1234,
                    'instances': [
                        'foo@hostX',
                        'bar@:1000',
                        'hostonly',
                        ':1234'
                    ]
                },
                'calls': [
                    call('hostX', 6379, None),
                    call('localhost', 1000, None),
                    call('hostonly', 6379, None),
                    call('localhost', 1234, None),
                ],
            },
        }

        for testname, data in testcases.items():
            config = get_collector_config('RedisCollector', data['config'])

            collector = RedisCollector(config, None)

            mock = Mock(return_value={}, name=testname)
            patch_c = patch.object(RedisCollector, 'collect_instance', mock)

            patch_c.start()
            collector.collect()
            patch_c.stop()

            expected_call_count = len(data['calls'])
            self.assertEqual(mock.call_count, expected_call_count,
                             msg='[%s] mock.calls=%d != expected_calls=%d' %
                             (testname, mock.call_count, expected_call_count))
            for exp_call in data['calls']:
                # Test expected calls 1 by 1,
                # because self.instances is a dict (=random order)
                mock.assert_has_calls(exp_call)

    @run_only_if_redis_is_available
    @patch.object(Collector, 'publish')
    def test_key_naming_when_using_instances(self, publish_mock):

        config_data = {
            'instances': [
                'nick1@host1:1111',
                'nick2@:2222',
                'nick3@host3',
                'nick4@host4:3333/@password',
                'bla'
            ]
        }
        get_info_data = {
            'total_connections_received': 200,
            'total_commands_processed': 100,
        }
        expected_calls = [
            call('process.connections_received', 200, precision=0,
                 metric_type='GAUGE', source='nick1'),
            call('process.commands_processed', 100, precision=0,
                 metric_type='GAUGE', source='nick1'),
            call('process.connections_received', 200, precision=0,
                 metric_type='GAUGE', source='nick2'),
            call('process.commands_processed', 100, precision=0,
                 metric_type='GAUGE', source='nick2'),
            call('process.connections_received', 200, precision=0,
                 metric_type='GAUGE', source='nick3'),
            call('process.commands_processed', 100, precision=0,
                 metric_type='GAUGE', source='nick3'),
            call('process.connections_received', 200, precision=0,
                 metric_type='GAUGE', source='nick4'),
            call('process.commands_processed', 100, precision=0,
                 metric_type='GAUGE', source='nick4'),
            call('process.connections_received', 200, precision=0,
                 metric_type='GAUGE', source='6379'),
            call('process.commands_processed', 100, precision=0,
                 metric_type='GAUGE', source='6379'),
        ]

        config = get_collector_config('RedisCollector', config_data)
        collector = RedisCollector(config, None)

        patch_c = patch.object(RedisCollector, '_get_info',
                               Mock(return_value=get_info_data))

        patch_c.start()
        collector.collect()
        patch_c.stop()

        self.assertEqual(publish_mock.call_count, len(expected_calls))
        for exp_call in expected_calls:
            # Test expected calls 1 by 1,
            # because self.instances is a dict (=random order)
            publish_mock.assert_has_calls(exp_call)


################################################################################
if __name__ == "__main__":
    unittest.main()
