# coding=utf-8

"""
[Librato](http://librato.com) is an infrastructure software as a service company
dedicated to delivering beautiful, easy to use tools that make managing your
operations more fun and efficient.

#### Dependencies

 * [librato-metrics](https://github.com/librato/python-librato)

#### Configuration

Enable this handler

 * handlers = diamond.handler.libratohandler.LibratoHandler,

 * user = LIBRATO_USERNAME
 * apikey = LIBRATO_API_KEY

 * queue_max_size = [optional | 300] max measurements to queue before submitting
 * queue_max_interval [optional | 60] @max seconds to wait before submitting
     For best behavior, be sure your highest collector poll interval is lower
     than or equal to the queue_max_interval setting.

 * include_filters = [optional | '^.*'] A list of regex patterns.
     Only measurements whose path matches a filter will be submitted.
     Useful for limiting usage to *only* desired measurements, e.g.
       include_filters = "^diskspace\..*\.byte_avail$", "^loadavg\.01"
       include_filters = "^sockets\.",
                                     ^ note trailing comma to indicate a list

"""

from Handler import Handler
import logging
import time
import re

try:
    import librato
    librato  # workaround for pyflakes issue #13
    librato.Queue.MAX_MEASUREMENTS_PER_CHUNK = 500
except ImportError:
    librato = None


class LibratoHandler(Handler):

    def __init__(self, config=None):
        """
        Create a new instance of the LibratoHandler class
        """
        # Initialize Handler
        Handler.__init__(self, config)
        logging.debug("Initialized Librato handler.")

        if librato is None:
            logging.error("Failed to load librato module")
            return

        # Initialize Options
        self.api = librato.connect(self.config['user'], self.config['apikey'])

        self.queue = []
        self.queue_max_age = int(self.config['queue_max_age'])
        self.queue_max_size = int(self.config['queue_max_size'])
        self.queue_max_interval = int(self.config['queue_max_interval'])
        self.queue_max_timestamp = int(time.time() + self.queue_max_interval)
        self.disable_force_flush = bool(self.config['disable_force_flush'])

        # If a user leaves off the ending comma, cast to a array for them
        include_filters = self.config['include_filters']
        if isinstance(include_filters, basestring):
            include_filters = [include_filters]

        self.include_reg = re.compile(r'(?:%s)' % '|'.join(include_filters))

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler
        """
        config = super(LibratoHandler, self).get_default_config_help()

        config.update({
            'user': '',
            'apikey': '',
            'queue_max_age': '',
            'queue_max_size': '',
            'queue_max_interval': '',
            'include_filters': '',
            'include_period': '',
            'enable_ssa': '',
            'disable_force_flush': ''
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler
        """
        config = super(LibratoHandler, self).get_default_config()

        config.update({
            'user': '',
            'apikey': '',
            'queue_max_age': 1800,
            'queue_max_size': 300,
            'queue_max_interval': 60,
            'include_filters': ['^.*'],
            'include_period': False,
            'enable_ssa': False,
            'disable_force_flush': False
        })

        return config

    def _get_path(self, metric):
        """
        Generate the path that should be logged for the metric.
        """
        path = metric.getCollectorPath()
        path += '.'
        path += metric.getMetricPath()

        return path

    def _get_queue_item(self, metric):
        """
        Generate an item to add to the queue.
        """
        path = self._get_path(metric)
        if metric.metric_type == 'GAUGE':
            m_type = 'gauge'
        else:
            m_type = 'counter'
        queue_args = {
            'name': path,
            'value': float(metric.value),
            'type': m_type,
            'source': metric.host,
            'measure_time': metric.timestamp
        }
        if self.config['include_period'] and metric.interval:
            queue_args['period'] = int(metric.interval)
        if self.config['enable_ssa']:
            queue_args['attributes'] = {'aggregate': True}

        return queue_args

    def process(self, metric):
        """
        Process a metric by sending it to Librato
        """
        path = self._get_path(metric)

        if self.include_reg.match(path):
            item = self._get_queue_item(metric)
            self.queue.append(item)
        else:
            self.log.debug("LibratoHandler: Skip %s, no include_filters match",
                           path)

        if not self.disable_force_flush:
            if (len(self.queue) >= self.queue_max_size or
                    time.time() >= self.queue_max_timestamp):
                self.log.debug("LibratoHandler: Sending batch size: %d",
                               len(self.queue))
                self._send()

    def flush(self):
        """Flush metrics in queue"""
        self._send()

    def _get_valid_items(self):
        """Get a list of items that are appropriate to send to Librato."""
        age_cutoff = time.time() - self.queue_max_age
        items = [q for q in self.queue if q['measure_time'] > age_cutoff]

        diff = len(self.queue) - len(items)
        if diff > 0:
            self.log.debug("LibratoHandler: Dropping %d expired items.", diff)

        return items

    def _send(self):
        """
        Send data to Librato.
        """
        # Make a new queue each time before submitting. There is no supported
        # way to clear a queue, so start from scratch each time.
        librato_queue = self.api.new_queue()
        for i in self._get_valid_items():
            librato_queue.add(**i)

        librato_queue.submit()

        self.queue_max_timestamp = int(time.time() + self.queue_max_interval)
        self.queue = []
