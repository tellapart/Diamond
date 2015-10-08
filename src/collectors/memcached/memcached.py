# coding=utf-8

"""
Collect memcached stats



#### Dependencies

 * subprocess

#### Example Configuration

MemcachedCollector.conf

```
    enabled = True
    hosts = localhost:11211, app-1@localhost:11212, app-2@localhost:11213, etc
```

TO use a unix socket, set a host string like this

```
    hosts = /path/to/blah.sock, app-1@/path/to/bleh.sock,
```
"""

import diamond.collector
import socket
import re


class Protocol(object):
    QUIT = 'quit\r\n'
    STATS = 'stats\r\n'
    STATS_RESET = 'stats reset\r\n'


class MemcachedCollector(diamond.collector.Collector):
    GAUGES = [
        'bytes',
        'connection_structures',
        'curr_connections',
        'curr_items',
        'threads',
        'reserved_fds',
        'limit_maxbytes',
        'hash_power_level',
        'hash_bytes',
        'hash_is_expanding',
        'uptime'
    ]
    GAUGES_RE = [
        re.compile('^latency:')
    ]

    def get_default_config_help(self):
        config_help = super(MemcachedCollector, self).get_default_config_help()
        config_help.update({
            'publish': "Which rows of 'status' you would like to publish."
            + " Telnet host port' and type stats and hit enter to see the list"
            + " of possibilities. Leave unset to publish all.",
            'hosts': "List of hosts, and ports to collect. Set an alias by "
            + " prefixing the host:port with alias@",
            'reset_stats': "If true, execute 'stats reset' against each "
                           "endpoint after collection."
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(MemcachedCollector, self).get_default_config()
        config.update({
            'path':     'memcached',

            # Which rows of 'status' you would like to publish.
            # 'telnet host port' and type stats and hit enter to see the list of
            # possibilities.
            # Leave unset to publish all
            #'publish': ''

            # Connection settings
            'hosts': ['localhost:11211'],
            'reset_stats': False
        })
        return config

    def get_socket(self, host, port, timeout=5):
        if port is None:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.connect(host)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((host, int(port)))

        sock.settimeout(timeout)
        return sock

    def get_raw_stats(self, host, port):
        data = ''
        # connect
        try:
            sock = self.get_socket(host, port)
            # request stats

            commands = [Protocol.STATS]
            if self.config['reset_stats']:
                self.log.debug('Resetting statistics on %s:%s', host, port)
                commands.append(Protocol.STATS_RESET)

            commands.append(Protocol.QUIT)
            sock.sendall(''.join(commands))
            while True:
                d = sock.recv(4096)
                if not d:
                    break
                data += d

        except socket.error:
            self.log.exception('Failed to get stats from %s:%s',
                               host, port)
        return data

    def get_stats(self, host, port):
        # stuff that's always ignored, aren't 'stats'
        ignored = ('libevent', 'pointer_size', 'time', 'version',
                   'repcached_version', 'replication', 'accepting_conns',
                   'pid')
        pid = None

        stats = {}
        data = self.get_raw_stats(host, port)

        # parse stats
        for line in data.splitlines():
            pieces = line.split(' ')
            if pieces[0] != 'STAT' or pieces[1] in ignored:
                continue
            elif pieces[1] == 'pid':
                pid = pieces[2]
                continue
            if '.' in pieces[2]:
                stats[pieces[1]] = float(pieces[2])
            else:
                stats[pieces[1]] = int(pieces[2])

        # get max connection limit
        self.log.debug('pid %s', pid)
        try:
            cmdline = "/proc/%s/cmdline" % pid
            f = open(cmdline, 'r')
            m = re.search("-c\x00(\d+)", f.readline())
            if m is not None:
                self.log.debug('limit connections %s', m.group(1))
                stats['limit_maxconn'] = m.group(1)
            f.close()
        except:
            self.log.debug("Cannot parse command line options for memcached")

        return stats

    def is_gauge(self, stat):
        # Everything is a gauge if stats are being reset on each pass.
        if self.config['reset_stats']:
            return True
        if stat in MemcachedCollector.GAUGES:
            return True
        for gauge_re in MemcachedCollector.GAUGES_RE:
            if gauge_re.search(stat):
                return True
        return False

    def collect(self):
        hosts = self.config.get('hosts')

        # Convert a string config value to be an array
        if isinstance(hosts, basestring):
            hosts = [hosts]

        for host in hosts:
            matches = re.search('((.+)\@)?([^:]+)(:(\d+))?', host)
            alias = matches.group(2)
            hostname = matches.group(3)
            port = matches.group(5)

            if alias is None:
                alias = hostname

            stats = self.get_stats(hostname, port)

            # figure out what we're configured to get, defaulting to everything
            desired = self.config.get('publish', stats.keys())

            # for everything we want
            for stat in desired:
                if stat in stats:

                    # we have it
                    formatted_name = '.'.join((alias, stat.replace(':', '.')))
                    if self.is_gauge(stat):
                        value = stats[stat]
                    else:
                        value = self.derivative(
                            name=formatted_name,
                            new=stats[stat],
                            time_delta=False)

                    self.publish_gauge(formatted_name, value)
                else:

                    # we don't, must be something configured in publish so we
                    # should log an error about it
                    self.log.error("No such key '%s' available, issue 'stats' "
                                   "for a full list", stat)
