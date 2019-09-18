import unittest
import os
import re
from mock import patch
from addict import Dict

from scheduler import config

class TestConfig(unittest.TestCase):

    def test_default_env(self):
        var_foo = config.default_env('foobar', 1, int)
        self.assertEqual(var_foo[1], 1)
        os.environ['FOOBAR'] = '99'
        var_foo2 = config.default_env('foobar', 1, int)
        self.assertEqual(var_foo2[1], 99)
    
    def test_product_frequency(self):
        frequency = config.product_frequency()
        self.assertEqual(frequency, ['landsat', 'landsat', 'landsat', 'modis', 'modis', 'viirs', 'plot'])
    
    def test_config(self):
        cfg = config.config()
        self.assertEqual(sorted(list(cfg.keys())),
                         sorted(['mesos_principal', 'mesos_secret', 'mesos_master', 'mesos_user', 'product_frequency',
                          'espa_api', 'product_request_count', 'product_request_frequency', 'product_scheduled_max', 
                          'max_cpu', 'task_cpu', 'task_mem', 'task_disk', 'task_image', 'offer_refuse_seconds', 
                          'auxiliary_mount', 'aux_dir', 'storage_mount', 'espa_storage', 'aster_ged_server_name', 
                          'handle_orders_frequency', 'log_level', 'urs_machine', 'urs_login', 'urs_password']))

