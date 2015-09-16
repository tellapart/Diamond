#!/usr/bin/python
# coding=utf-8
################################################################################

from mock import patch
from subprocess import CalledProcessError
from test import unittest
import configobj

from diamond.collector import (
    Collector,
    reset_hostname_cache
)
from diamond.error import DiamondException


class BaseCollectorTest(unittest.TestCase):

    def test_SetCustomHostname(self):
        reset_hostname_cache()
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
        reset_hostname_cache()
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
    def test_SetHostnameViaShellCmdWithExpiration(self, patch_time):
        reset_hostname_cache()
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

    @patch('time.time')
    def test_SetHostnameViaShellCmdWithError(self, patch_time):
        reset_hostname_cache()
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'exit 1',
            'hostname_method': 'shell',
            'hostname_cache_expiration_interval': '300'
        }
        # Should fail the first time.
        c = Collector(config, [])
        self.assertRaises(CalledProcessError, c.get_hostname)

        # Success
        patch_time.return_value = 0
        config['collectors']['default']['hostname'] = 'echo custom.localhost'
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

        # Should NOT fail before the timeout.
        patch_time.return_value = 299
        config['collectors']['default']['hostname'] = 'exit 1'
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

        # Should fail again after the timeout.
        patch_time.return_value = 301
        config['collectors']['default']['hostname'] = 'exit 1'
        c = Collector(config, [])
        self.assertRaises(CalledProcessError, c.get_hostname)

    @patch('time.time')
    def test_SetHostnameViaShellCmdWithErrorSkip(self, patch_time):
        reset_hostname_cache()
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'exit 1',
            'hostname_method': 'shell',
            'hostname_cache_expiration_interval': '300',
            'hostname_cache_skip_errors': True
        }
        # Should fail the first time.
        c = Collector(config, [])
        self.assertRaises(CalledProcessError, c.get_hostname)

        # Success
        patch_time.return_value = 0
        config['collectors']['default']['hostname'] = 'echo custom.localhost'
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

        # Should NOT fail before the timeout.
        patch_time.return_value = 299
        config['collectors']['default']['hostname'] = 'exit 1'
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

        # Should NOT fail again after the timeout.
        patch_time.return_value = 301
        config['collectors']['default']['hostname'] = 'exit 1'
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())

    def test_SetHostnameViaShellJson(self):
        reset_hostname_cache()
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'python -c "import json; print json.dumps({\'test_key\': \'test_value\'})"',
            'hostname_method': 'shell',
            'shell_json_key': 'test_key'
        }

        c = Collector(config, [])
        self.assertEquals('test_value', c.get_hostname())

        reset_hostname_cache()
        config['collectors']['default']['shell_json_key'] = 'bad_key'
        c = Collector(config, [])

        self.assertRaisesRegexp(
            DiamondException, 'No value found for shell key', c.get_hostname)

    @patch('time.time')
    def test_SetHostnameViaShellCmdWithErrorSkipJsonKey(self, patch_time):
        reset_hostname_cache()
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'exit 1',
            'hostname_method': 'shell',
            'hostname_cache_expiration_interval': '300',
            'hostname_cache_skip_errors': True,
            'shell_json_key': 'test_key'
        }
        # Should fail the first time.
        c = Collector(config, [])
        self.assertRaises(CalledProcessError, c.get_hostname)

        script = 'python -c "import json; print json.dumps({\'test_key\': \'test_value\'})"'

        # Success
        patch_time.return_value = 0
        config['collectors']['default']['hostname'] = script
        c = Collector(config, [])
        self.assertEquals('test_value', c.get_hostname())

        # Should NOT fail before the timeout.
        patch_time.return_value = 299
        config['collectors']['default']['hostname'] = 'exit 1'
        c = Collector(config, [])
        self.assertEquals('test_value', c.get_hostname())

        # Should NOT fail again after the timeout.
        patch_time.return_value = 301
        config['collectors']['default']['hostname'] = 'exit 1'
        c = Collector(config, [])
        self.assertEquals('test_value', c.get_hostname())

    def test_SourceMerge(self):
        reset_hostname_cache()
        config = configobj.ConfigObj()
        config['server'] = {}
        config['server']['collectors_config_path'] = ''
        config['collectors'] = {}
        config['collectors']['default'] = {
            'hostname': 'custom.localhost',
        }
        c = Collector(config, [])
        self.assertEquals('custom.localhost', c.get_hostname())
        self.assertEquals('custom.localhost', c.construct_host(None))
        self.assertEquals('test', c.construct_host('test'))

        config['collectors']['default']['merge_sources'] = True
        c = Collector(config, [])
        self.assertEquals('custom.localhost.test', c.construct_host('test'))

        config['collectors']['default']['merge_sources_separator'] = '!'
        c = Collector(config, [])
        self.assertEquals('custom.localhost!test', c.construct_host('test'))
