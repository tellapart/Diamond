# coding=utf-8

"""
Simple collector which get JSON and parse it into flat metrics

#### Dependencies

 * urllib2

"""

import urllib2
import json
import diamond.collector


class HTTPJSONCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(HTTPJSONCollector, self).get_default_config_help()
        config_help.update({
            'url': 'Full URL'
        })
        return config_help

    def get_default_config(self):
        default_config = super(HTTPJSONCollector, self).get_default_config()
        default_config.update({
            'path': 'httpjson',
            'url': 'http://localhost/stat',
            'timeout': 10
        })
        return default_config

    def _json_to_flat_metrics(self, prefix, data):
        for key, value in data.items():
            if prefix:
                final_key = "%s.%s" % (prefix, key)
            else:
                final_key = key
            if isinstance(value, dict):
                for k, v in self._json_to_flat_metrics(
                        final_key, value):
                    yield k, v
            else:
                try:
                    float(value)
                except ValueError:
                    value = None
                finally:
                    yield (final_key, value)

    def collect(self):
        url_config = self.config['url']
        timeout = self.config['timeout']

        if isinstance(url_config, basestring):
            url_config = {url_config: {}}

        for url, options in url_config.items():
            req = urllib2.Request(url)
            req.add_header('Content-type', 'application/json')
            service = options.get('service')

            self.log.info(
                'Fetching data from %s with service override: %s.',
                url,
                service
            )
            try:
                resp = urllib2.urlopen(req, timeout=timeout)
            except Exception as e:
                self.log.error("Can't open url %s. %s", url, e)
            else:

                content = resp.read()

                try:
                    data = json.loads(content)
                except ValueError as e:
                    self.log.error(
                        "Can't parse JSON object from %s. %s", url, e)
                else:
                    for m_name, m_value in self._json_to_flat_metrics("", data):
                        self.publish(m_name, m_value, service=service)
