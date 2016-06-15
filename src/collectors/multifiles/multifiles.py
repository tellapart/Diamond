# coding=utf-8

"""
Collects data from an arbitrary set of files.

Configuration is nested, and allows for per-file options. The default parsing
mode for a file is yaml, which also supports JSON. For YAML/JSON files, the
format is simple key:value, where value is parseable as a float. The following
files would be considered valid:


File1:
---
metric1: 123
metric2: 456

File2:
{
    "metric1": 123,
    "metric2": 456
}

The current valid options are:

* service - The specific name of the service that data is being collected for.

Given a directory structure as follows:

/tmp
├── service1
│   ├── data1.yaml
│   └── data2.yaml
├── service2.yaml

The following configuration would record stats from /tmp/service1/data1.yaml
and /tmp/service1/data2.yaml under "service1" and stats for from
/tmp/service2.yaml under service2:

[files]
    [[/tmp/service1]]
        service = service1
    [[/tmp/service2.yaml]]
        service = service2
"""

import diamond.collector
import yaml
import os


class MultiFilesCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(MultiFilesCollector, self).get_default_config_help()
        config_help.update({
            'files': 'A dictionary of files or paths to collect data from.'
        })
        return config_help

    def get_default_config(self):
        """
        Returns default collector settings.
        """
        config = super(MultiFilesCollector, self).get_default_config()
        config.update({
            'files': []
        })
        return config

    def collect(self):
        paths = self._expand_paths(self.config.get('files', {}))
        self.log.debug('Collecting from files: %s', paths.keys())
        for path, options in paths.items():
            try:
                service = options.get('service')
                with open(path, 'r') as f:
                    data = yaml.safe_load(f)
                for k, v in (data or {}).items():
                    try:
                        value = float(v)
                    except ValueError:
                        value = None
                    self.publish(k, value, service=service)
            except:
                self.log.exception('Unable to process file: %s', path)

    def _expand_paths(self, config):
        all_paths = {}
        for path, options in config.items():
            if os.path.exists(path):
                if os.path.isfile(path):
                    all_paths[path] = options
                else:
                    for fn in os.listdir(path):
                        fp = os.path.join(path, fn)
                        if os.path.isfile(fp):
                            all_paths[fp] = options
            else:
                self.log.warn('Unable to find file or directory at: %s', path)

        return all_paths
