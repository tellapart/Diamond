# coding=utf-8

"""
The Collector class is a base class for all metric collectors.
"""

import json
import os
import socket
import platform
import logging
import configobj
import traceback
import time
import re
import sys

from diamond.metric import Metric
from error import DiamondException

if os.name == 'posix' and sys.version_info[0] < 3:
    import subprocess as subprocess_old
    import subprocess32 as subprocess
    subprocess._args_from_interpreter_flags = subprocess_old._args_from_interpreter_flags
    sys.modules["subprocess"] = subprocess
    del subprocess_old
else:
    import subprocess

# Detect the architecture of the system and set the counters for MAX_VALUES
# appropriately. Otherwise, rolling over counters will cause incorrect or
# negative values.

if platform.architecture()[0] == '64bit':
    MAX_COUNTER = (2 ** 64) - 1
else:
    MAX_COUNTER = (2 ** 32) - 1

LOG = logging.getLogger('diamond')


def get_hostname(config, method=None):
    """
    Returns a hostname as configured by the user
    """
    method = method or config.get('hostname_method', 'smart')

    # case insensitive method
    method = method.lower()

    if 'hostname' in config and method != 'shell':
        return config['hostname']

    if method in get_hostname.cached_results:
        hostname, last_cached = get_hostname.cached_results[method]
        interval = config.get('hostname_cache_expiration_interval')
        if not interval or (time.time() - last_cached) < int(interval):
            return hostname

    if method == 'shell':
        if 'hostname' not in config:
            raise DiamondException(
                "hostname must be set to a shell command for"
                " hostname_method=shell")
        else:
            proc = subprocess.Popen(config['hostname'],
                                    shell=True,
                                    stdout=subprocess.PIPE)
            raw_hostname = proc.communicate(timeout=5)[0].strip()
            if proc.returncode == 0:  # Only try to parse if call successful.
                hostname = _parse_shell_hostname(config, raw_hostname)
            else:
                skip_errors = config.get('hostname_cache_skip_errors')
                # If we aren't explicitly skipping errors OR we have no cached
                # result, raise an exception.
                if not skip_errors or method not in get_hostname.cached_results:
                    raise subprocess.CalledProcessError(proc.returncode,
                                                        config['hostname'])
                else:  # Recycle the previously cached version.
                    hostname = get_hostname.cached_results[method][0]
                    LOG.error('Unable to reload hostname. Return code: %s',
                              proc.returncode)

    elif method == 'smart':
        hostname = get_hostname(config, 'fqdn_short')
        if hostname == 'localhost':
            hostname = get_hostname(config, 'hostname_short')

    elif method == 'fqdn_short':
        hostname = socket.getfqdn().split('.')[0]

    elif method == 'fqdn':
        hostname = socket.getfqdn().replace('.', '_')

    elif method == 'fqdn_rev':
        hostname = socket.getfqdn().split('.')
        hostname.reverse()
        hostname = '.'.join(hostname)

    elif method == 'uname_short':
        hostname = os.uname()[1].split('.')[0]

    elif method == 'uname_rev':
        hostname = os.uname()[1].split('.')
        hostname.reverse()
        hostname = '.'.join(hostname)

    elif method == 'hostname':
        hostname = socket.gethostname()

    elif method == 'hostname_short':
        hostname = socket.gethostname().split('.')[0]

    elif method == 'hostname_rev':
        hostname = socket.gethostname().split('.')
        hostname.reverse()
        hostname = '.'.join(hostname)

    elif method == 'none':
        hostname = None

    else:
        raise NotImplementedError(config['hostname_method'])

    if hostname == '':
        raise DiamondException('Hostname is empty?!')

    current_time = time.time()
    get_hostname.cached_results[method] = (hostname, current_time)
    return hostname

get_hostname.cached_results = {}

def _parse_shell_hostname(config, raw_hostname):
    """Parses the hostname from shell output based on configuration settings.
    """
    shell_json_key = config.get('shell_json_key')
    if shell_json_key:
        result = json.loads(raw_hostname).get(shell_json_key)
        if result is None:
            raise DiamondException(
                'No value found for shell key %s.' % shell_json_key)
        return result

    return raw_hostname

def reset_hostname_cache():
    """Resets the hostname cache. Useful for unit tests.
    """
    get_hostname.cached_results = {}

def str_to_bool(value):
    """
    Converts string truthy/falsey strings to a bool
    Empty strings are false
    """
    if isinstance(value, basestring):
        value = value.strip().lower()
        if value in ['true', 't', 'yes', 'y']:
            return True
        elif value in ['false', 'f', 'no', 'n', '']:
            return False
        else:
            raise NotImplementedError("Unknown bool %s" % value)

    return value


class CollectionResult(object):
    """
    Represents the result of a collection operation.
    """

    def __init__(self, success=True, timestamp=None, children=None, alias=None,
                 error=None):
        """
        Args:
            success - Whether or not collection succeeded.
            timestamp - The optional time of collection.
            children - An optional list of child CollectionResults.
            alias - An optional alias for the CollectionResult.
            error - The specific error that occured.
        """
        self._success = success
        self.timestamp = timestamp or time.time()
        self.children = children or []
        self.alias = alias or 'default'
        self.error = error

    @property
    def success(self):
        """
        Returns whether or not the overall collection was a success.
        """
        if not self.children:
            return self._success

        return all(c.success for c in self.children)


class Collector(object):
    """
    The Collector class is a base class for all metric collectors.
    """

    def __init__(self, config, handlers):
        """
        Create a new instance of the Collector class
        """
        # Initialize Logger
        self.log = logging.getLogger('diamond')
        # Initialize Members
        self.name = self.__class__.__name__
        self.handlers = handlers
        self.last_values = {}

        self.last_result = None

        # Get Collector class
        cls = self.__class__

        # Initialize config
        self.config = configobj.ConfigObj()

        # Check if default config is defined
        if self.get_default_config() is not None:
            # Merge default config
            self.config.merge(self.get_default_config())

        # Merge default Collector config
        self.config.merge(config['collectors']['default'])

        # Check if Collector config section exists
        if cls.__name__ in config['collectors']:
            # Merge Collector config section
            self.config.merge(config['collectors'][cls.__name__])

        if not config.get('ignore_config_files', False):
            # Check for config file in config directory
            configfile = os.path.join(config['server']['collectors_config_path'],
                                      cls.__name__) + '.conf'
            if os.path.exists(configfile):
                # Merge Collector config file
                self.config.merge(configobj.ConfigObj(configfile))

        # Handle some config file changes transparently
        if isinstance(self.config['byte_unit'], basestring):
            self.config['byte_unit'] = self.config['byte_unit'].split()

        self.config['enabled'] = str_to_bool(self.config['enabled'])

        self.config['measure_collector_time'] = str_to_bool(
            self.config['measure_collector_time'])

        # Raise an error if both whitelist and blacklist are specified
        if self.config['metrics_whitelist'] and \
                self.config['metrics_blacklist']:
                raise DiamondException(
                    'Both metrics_whitelist and metrics_blacklist specified ' +
                    'in file %s' % configfile)

        if self.config['metrics_whitelist']:
            self.config['metrics_whitelist'] = re.compile(
                self.config['metrics_whitelist'])
        elif self.config['metrics_blacklist']:
            self.config['metrics_blacklist'] = re.compile(
                self.config['metrics_blacklist'])

        self.collect_running = False

        self.groups = self.config.get('groups')
        if isinstance(self.groups, basestring):
            self.groups = [self.groups]

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this collector
        """
        return {
            'enabled': 'Enable collecting these metrics',
            'byte_unit': 'Default numeric output(s)',
            'measure_collector_time': 'Collect the collector run time in ms',
            'metrics_whitelist': 'Regex to match metrics to transmit. ' +
                                 'Mutually exclusive with metrics_blacklist',
            'metrics_blacklist': 'Regex to match metrics to block. ' +
                                 'Mutually exclusive with metrics_whitelist',
        }

    def get_default_config(self):
        """
        Return the default config for the collector
        """
        return {
            # Defaults options for all Collectors

            # Uncomment and set to hardcode a hostname for the collector path
            # Keep in mind, periods are separators in graphite
            # 'hostname': 'my_custom_hostname',

            # If you prefer to just use a different way of calculating the
            # hostname
            # Uncomment and set this to one of these values:
            # fqdn_short  = Default. Similar to hostname -s
            # fqdn        = hostname output
            # fqdn_rev    = hostname in reverse (com.example.www)
            # uname_short = Similar to uname -n, but only the first part
            # uname_rev   = uname -r in reverse (com.example.www)
            # 'hostname_method': 'fqdn_short',

            # All collectors are disabled by default
            'enabled': False,

            # Path Prefix
            'path_prefix': 'servers',

            # Path Prefix for Virtual Machine metrics
            'instance_prefix': 'instances',

            # Path Suffix
            'path_suffix': '',

            # Default splay time (seconds)
            'splay': 1,

            # Default Poll Interval (seconds)
            'interval': 300,

            # Default Event TTL (interval multiplier)
            'ttl_multiplier': 2,

            # Default collector threading model
            'method': 'Sequential',

            # Default numeric output
            'byte_unit': 'byte',

            # Collect the collector run time in ms
            'measure_collector_time': False,

            # Whitelist of metrics to let through
            'metrics_whitelist': None,

            # Blacklist of metrics to let through
            'metrics_blacklist': None,

            # If true, merge the hostname + provided source.
            'merge_sources': False,

            # The separator character to use if merging sources.
            'merge_sources_separator': '.'
        }

    def get_stats_for_upload(self, config=None):
        if config is None:
            config = self.config

        stats = {}

        if 'enabled' in config:
            stats['enabled'] = config['enabled']
        else:
            stats['enabled'] = False

        if 'interval' in config:
            stats['interval'] = config['interval']

        return stats

    def get_schedule(self):
        """
        Return schedule for the collector
        """
        # Return a dict of tuples containing (collector function,
        # collector function args, splay, interval)
        return {self.__class__.__name__: (self._run,
                                          None,
                                          int(self.config['splay']),
                                          int(self.config['interval']))}

    def get_last_result(self):
        """Gets the last viable result.

        If the last result hasn't been updated in 3x the polling interval, it's
        possible something has gone wrong with the scheduling.
        """
        if not self.last_result:
            return None

        interval = int(self.config['interval'])
        now = time.time()
        if self.last_result.timestamp < (now - interval * 3):
            return None
        else:
            return self.last_result

    def get_metric_path(self, name, instance=None, source=None):
        """
        Get metric path.
        Instance indicates that this is a metric for a
            virtual machine and should have a different
            root prefix.
        """
        if 'path' in self.config:
            path = self.config['path']
        else:
            path = self.__class__.__name__

        if instance is not None:
            if 'instance_prefix' in self.config:
                prefix = self.config['instance_prefix']
            else:
                prefix = 'instances'
            if path == '.':
                return '.'.join([prefix, instance, name])
            else:
                return '.'.join([prefix, instance, path, name])

        if 'path_prefix' in self.config:
            prefix = self.config['path_prefix']
        else:
            prefix = 'systems'

        if 'path_suffix' in self.config:
            suffix = self.config['path_suffix']
        else:
            suffix = None

        hostname = self.construct_host(source)
        if hostname is not None:
            if prefix:
                prefix = ".".join((prefix, hostname))
            else:
                prefix = hostname

        # if there is a suffix, add after the hostname
        if suffix:
            prefix = '.'.join((prefix, suffix))

        if path == '.':
            return '.'.join([prefix, name])
        else:
            return '.'.join([prefix, path, name])

    def get_hostname(self):
        return get_hostname(self.config)

    def collect(self):
        """
        Default collector method
        """
        raise NotImplementedError()

    def publish(self, name, value, raw_value=None, precision=0,
                metric_type='GAUGE', instance=None, source=None, service=None,
                groups=None):
        """
        Publish a metric with the given name
        """
        # Check whitelist/blacklist
        if self.config['metrics_whitelist']:
            if not self.config['metrics_whitelist'].match(name):
                return
        elif self.config['metrics_blacklist']:
            if self.config['metrics_blacklist'].match(name):
                return

        # Get metric Path
        path = self.get_metric_path(name, instance=instance, source=source)

        # Get metric TTL
        ttl = float(self.config['interval']) * float(
            self.config['ttl_multiplier'])

        interval = int(self.config['interval'])

        # Create Metric
        try:
            host = self.construct_host(source)
            service = service or self.config.get('service')
            groups = groups or self.groups
            metric = Metric(path, value, raw_value=raw_value, timestamp=None,
                            precision=precision, host=host,
                            metric_type=metric_type, ttl=ttl, interval=interval,
                            service=service, raw_name=name, groups=groups)
        except DiamondException:
            self.log.error(('Error when creating new Metric: path=%r, '
                            'value=%r'), path, value)
            raise

        # Publish Metric
        self.publish_metric(metric)

    def construct_host(self, source):
        """Create the hostname based on configuration settings.
        """
        if self.config['merge_sources']:
            separator = self.config['merge_sources_separator']
            return separator.join(
                i for i in (self.get_hostname(), source) if i)
        else:
            return source or self.get_hostname()

    def publish_metric(self, metric):
        """
        Publish a Metric object
        """
        # Process Metric
        for handler in self.handlers:
            handler._process(metric)

    def publish_gauge(self, name, value, precision=0, instance=None):
        return self.publish(name, value, precision=precision,
                            metric_type='GAUGE', instance=instance)

    def publish_counter(self, name, value, precision=0, max_value=0,
                        time_delta=True, interval=None, allow_negative=False,
                        instance=None):
        raw_value = value
        value = self.derivative(name, value, max_value=max_value,
                                time_delta=time_delta, interval=interval,
                                allow_negative=allow_negative,
                                instance=instance)
        return self.publish(name, value, raw_value=raw_value,
                            precision=precision, metric_type='COUNTER',
                            instance=instance)

    def derivative(self, name, new, max_value=0,
                   time_delta=True, interval=None, timestamp=None,
                   allow_negative=False, instance=None, source=None):
        """
        Calculate the derivative of the metric.
        """
        # Format Metric Path
        path = self.get_metric_path(name, instance=instance)

        if (path, source) in self.last_values:
            old, old_timestamp = self.last_values[(path, source)]
            # Check for rollover
            if new < old:
                old = old - max_value
            # Get Change in X (value)
            derivative_x = new - old

            # If we pass in a interval, use it rather then the configured one
            if timestamp is not None:
                interval = timestamp - old_timestamp
            elif interval is None:
                interval = int(self.config['interval'])

            # Get Change in Y (time)
            if time_delta:
                derivative_y = interval
            else:
                derivative_y = 1

            result = float(derivative_x) / float(derivative_y)
            if result < 0 and not allow_negative:
                result = 0
        else:
            result = 0

        # Store Old Value
        self.last_values[(path, source)] = (new, timestamp)

        # Return result
        return result

    def _run(self):
        """
        Run the collector unless it's already running
        """
        if self.collect_running:
            return
        # Log
        self.log.debug("Collecting data from: %s" % self.__class__.__name__)
        try:
            try:
                start_time = time.time()
                self.collect_running = True

                # Collect Data
                self.last_result = self.collect()

                end_time = time.time()

                if 'measure_collector_time' in self.config:
                    if self.config['measure_collector_time']:
                        metric_name = 'collector_time_ms'
                        metric_value = int((end_time - start_time) * 1000)
                        self.publish(metric_name, metric_value)

            except Exception:
                # Log Error
                error = traceback.format_exc()
                self.log.error(error)
                self.last_result = CollectionResult(success=False, error=error)
        finally:
            self.collect_running = False
            # After collector run, invoke a flush
            # method on each handler.
            for handler in self.handlers:
                handler._flush()

    def find_binary(self, binary):
        """
        Scan and return the first path to a binary that we can find
        """
        if os.path.exists(binary):
            return binary

        # Extract out the filename if we were given a full path
        binary_name = os.path.basename(binary)

        # Gather $PATH
        search_paths = os.environ['PATH'].split(':')

        # Extra paths to scan...
        default_paths = [
            '/usr/bin',
            '/bin'
            '/usr/local/bin',
            '/usr/sbin',
            '/sbin'
            '/usr/local/sbin',
        ]

        for path in default_paths:
            if path not in search_paths:
                search_paths.append(path)

        for path in search_paths:
            if os.path.isdir(path):
                filename = os.path.join(path, binary_name)
                if os.path.exists(filename):
                    return filename

        return binary


class ProcessCollector(Collector):
    """
    Collector with helpers for handling running commands with/without sudo
    """

    def get_default_config_help(self):
        config_help = super(ProcessCollector, self).get_default_config_help()
        config_help.update({
            'use_sudo':     'Use sudo?',
            'sudo_cmd':     'Path to sudo',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(ProcessCollector, self).get_default_config()
        config.update({
            'use_sudo':     False,
            'sudo_cmd':     self.find_binary('/usr/bin/sudo'),
        })
        return config

    def run_command(self, args):
        if 'bin' not in self.config:
            raise Exception('config does not have any binary configured')
        try:
            command = args
            command.insert(0, self.config['bin'])

            if str_to_bool(self.config['use_sudo']):
                command.insert(0, self.config['sudo_cmd'])

            return subprocess.Popen(command,
                                    stdout=subprocess.PIPE).communicate()
        except OSError:
            self.log.exception("Unable to run %s", command)
            return None
