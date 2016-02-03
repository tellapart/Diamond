# coding=utf-8

"""
Handler for writing metrics to observability.

#### Dependencies

 * oauthlib
 * requests
 * requests-oauthlib

#### Configuration

Enable this handler:

 * handlers = diamond.handler.observability.ObservabilityHandler,

Configuration options:

 * client_key = <Consumer Key>
 * client_secret = <Consumer Secret>
 * zone = <Metrics Zone>
 * token_url = <OAuth2 Token URL>
 * relay_url = <Metrics Relay URL>
 * queue_max_age = <Maximum age of an item in the flush queue>
 * floor_seconds = <Rounding interval for grouping metrics by time>

Note that the handler will never flush per-write, only at scheduled intervals.
"""

from Handler import Handler
from itertools import groupby
import json
import logging
import time

try:
    from oauthlib import oauth2
    import requests_oauthlib
except ImportError:
    oauth2 = None
    requests_oauthlib = None

class ObservabilityHandler(Handler):

    def __init__(self, config=None):
        """
        Create a new instance of the ObservabilityHandler class
        """
        # Initialize Handler
        Handler.__init__(self, config)
        logging.debug("Initialized Observability handler.")

        if oauth2 is None:
            logging.error("Failed to load oauthlib module")
            return

        if requests_oauthlib is None:
            logging.error("Failed to load requests_oauthlib module")
            return

        # Initialize client
        self.client_key = self.config['client_key']
        self.client_secret = self.config['client_secret']
        self.floor_seconds = int(self.config['floor_seconds'])

        self.queue = []
        self.queue_max_age = int(self.config['queue_max_age'])

        client = oauth2.BackendApplicationClient(client_id=self.client_key)
        self.session = requests_oauthlib.OAuth2Session(client=client)
        self.token = None

    def get_default_config_help(self):
        """
        Returns the help text for the configuration options for this handler.
        """
        config = super(ObservabilityHandler, self).get_default_config_help()

        config.update({
            'client_key': '',
            'client_secret': '',
            'zone': '',
            'token_url': '',
            'relay_url': '',
            'queue_max_age': '',
            'floor_seconds': ''
        })

        return config

    def get_default_config(self):
        """
        Return the default config for the handler.
        """
        config = super(ObservabilityHandler, self).get_default_config()

        config.update({
            'client_key': '',
            'client_secret': '',
            'zone': '',
            'token_url': '',
            'relay_url': '',
            'queue_max_age': 180,
            'floor_seconds': 10
        })

        return config

    def _get_queue_item(self, metric):
        """
        Generate an item to add to the queue.
        """
        queue_args = {
            'service': metric.getService(),
            'name': metric.getName(),
            'value': float(metric.value),
            'source': metric.getHost(),
            'measure_time': metric.timestamp
        }

        return queue_args

    def process(self, metric):
        """
        Queue the metric for later processing.
        """
        item = self._get_queue_item(metric)
        self.queue.append(item)

    def flush(self):
        """
        Flush metrics in queue.
        """
        self._send()

    def _get_valid_items(self):
        """
        Get a list of items that are appropriate to send to Observability.
        """
        age_cutoff = time.time() - self.queue_max_age
        items = [q for q in self.queue if q['measure_time'] > age_cutoff]

        diff = len(self.queue) - len(items)
        if diff > 0:
            self.log.debug("Dropping %d expired items.", diff)

        self.queue = []
        return items

    def _floor_timestamp(self, ts):
        return (int(ts) / self.floor_seconds) * self.floor_seconds

    def _create_request_groups(self, items):
        """Creates groups of requests to be posted in bulk.

        The body of a request to obs looks like:
        {
            'service': 'service',
            'source': 'source',
            'zone': 'zone',
            'current_time': 123,
            'metrics': {
                'key': 123
            }
        }

        So service/source/time are the grouping keys. Time will be rounded to
        the nearest floor_seconds so that we don't create too many batches.
        """
        groups = {}
        grouper = lambda v: (
            v['service'], v['source'], self._floor_timestamp(v['measure_time']))
        items = sorted(items, key=grouper)
        for k, g in groupby(items, grouper):
            groups[k] = list(g)

        return groups

    def _send(self):
        """
        Send data to Observability.
        """
        items = self._get_valid_items()
        for group, items in self._create_request_groups(items).iteritems():
            try:
                if not self.token:
                    self.token = self.session.fetch_token(
                        token_url=self.config['token_url'],
                        auth=(self.client_key, self.client_secret))

                service, source, timestamp = group
                metrics = {i['name']: i['value'] for i in items}
                data = dict(
                    zone=self.config['zone'],
                    service=service,
                    source=source,
                    timestamp=timestamp,
                    metrics=json.dumps(metrics))
                res = self.session.post(
                    url=self.config['relay_url'],
                    data=data)
                res.raise_for_status()
                self.log.debug(
                    'Successfully flushed %s metrics to the relay.', len(items))
            except Exception:
                self.log.exception('Error writing to metrics relay.')
                # Re-queue the items. If the POST is failing because of a bad
                # item it will eventually age out of the queue so we won't
                # be stuck forever.
                self.queue.extend(items)
                # The token is possibly invalid; re-fetch it.
                self.token = None
