#!/usr/bin/python
# coding=utf-8
################################################################################

from mock import patch
from test import unittest
import configobj

from diamond.collector import Collector


class BaseCollectorTest(unittest.TestCase):

    def test_SetCustomHostname(self):
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'custom.localhost',
        }
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

    def test_SetHostnameViaShellCmd(self):
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'echo custom.localhost',
            'hostname_method': 'shell',
        }
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

    @patch('time.time')
    def test_SetHostnameViaShellCmd(self, patch_time):
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'echo custom.localhost',
            'hostname_method': 'shell',
            'hostname_cache_expiration_interval': '300'
        }
        patch_time.return_value = 0
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

        patch_time.return_value = 100
        config['collectors']['default']['hostname'] = 'echo custom.localhost2'
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

        patch_time.return_value = 301
        config['collectors']['default']['hostname'] = 'echo custom.localhost2'
        c = Collector(config, [])
        self.assertEquals('custom.localhost2', c.get_hostname())
