# coding=utf-8

"""
A collector for publishing Scribe metrics from a local instance.

#### Dependencies

 * A local install of scribe_ctrl.
"""

import subprocess
import diamond.collector


class ScribeCollector(diamond.collector.Collector):

    # Mapping of known raw names to counter names
    COUNTERS = {
        'denied for queue size': 'denied_queue_size',
        'denied for rate': 'denied_rate',
        'lost': 'lost',
        'received bad': 'received_bad',
        'received blank category': 'received_blank_category',
        'received good': 'received_good',
        'requeue': 'requeue',
        'retries': 'retries',
        'sent': 'sent'
    }

    def get_default_config_help(self):
        config_help = super(ScribeCollector, self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(ScribeCollector, self).get_default_config()
        config.update({
            'path': 'scribe',
            'ports': '1464',
            'merge_sources': True,
            'bin': 'scribe_ctrl'
        })
        return config

    def _parse_raw_metrics(self, metrics):
        """
        Convert raw scribe output to a dictionary of metric names/values.
        """
        raw_items = [p for p in metrics.split('\n') if p]
        data = {}
        for item in raw_items:
            source, name, value = [s.strip() for s in item.split(':')]
            mapped_name = self.COUNTERS.get(name)
            if mapped_name:
                data['%s.%s' % (mapped_name, source)] = long(value)
            else:
                self.log.warn('Unexpected counter name: %s', name)

        return data

    def _get_metrics(self, port):
        """
        Get metrics from a specific port.
        """
        command = [self.config['bin'], 'counters', str(port)]
        try:
            proc = subprocess.Popen(command, stdout=subprocess.PIPE)
            out, _ = proc.communicate()

            return self._parse_raw_metrics(out)
        except Exception:
            self.log.exception(
                'Unable to collect scribe metrics on port %s.', port)
            return {}

    def collect(self):
        """
        Collects local scribe metrics for all configured ports.
        """
        ports = self.config['ports']
        if isinstance(ports, basestring):
            ports = [ports]

        ports = [int(p) for p in ports]

        for p in ports:
            metrics = self._get_metrics(p)
            for name, value in metrics.iteritems():
                # Only explicitly publish a source if there are multiple ports.
                source = str(p) if len(ports) > 1 else None
                # Everything is a counter, so calculate the derivative.
                value = self.derivative(
                    name=name,
                    new=value,
                    time_delta=False,
                    source=source)
                self.publish(name, value, source=source)
