# coding=utf-8

import json
import urllib2

import diamond.collector

class TeamCityCollector(diamond.collector.Collector):
    def get_default_config_help(self):
        config_help = super(TeamCityCollector, self).get_default_config_help()
        config_help.update({
            'hosts': 'A list of teamcity hosts to monitor, in host:port form.'
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(TeamCityCollector, self).get_default_config()
        config.update({
            'hosts': ['localhost:8111'],
            'path': 'teamcity'
        })
        return config

    def collect(self):
        """
        Collect TeamCity data
        """
        hosts = self.config.get('hosts')
        if isinstance(hosts, basestring):
            hosts = [hosts]

        for host in hosts:
            server_up = self._ping_server(host)
            connected_agents = self._get_connected_agents(host)
            self.publish('server.available', server_up)
            self.publish('server.connected_agents', connected_agents)

    def _fetch_data(self, host, url):
        """
        Fetch data from an endpoint and return the parsed JSON result.
        """
        url = 'http://%s/%s' % (host, url)
        headers = {
            'User-Agent': 'Diamond Mesos Collector',
            'Accept': 'application/json,*/*'
        }

        self.log.debug('Requesting TeamCity data from: %s' % url)
        req = urllib2.Request(url, headers=headers)
        handle = urllib2.urlopen(req)
        return handle.read()

    def _ping_server(self, host):
        try:
            data = self._fetch_data(host, 'guestAuth/app/rest/server/version')
            return 1 if data else 0
        except:
            self.log.exception("Error getting server version (ping)")
            return 0

    def _get_connected_agents(self, host):
        try:
            data = self._fetch_data(
                host,
                'guestAuth/app/rest/agents?locator=enabled:true,authorized:true')
            data = json.loads(data)
            return data['count']
        except:
            self.log.exception("Error getting connected agents")
            return 0
