import json
import os
import re
import requests
import requests_mock
import unittest

from mock import patch

from scheduler.main import EspaScheduler, get_framework
from scheduler.config import config
from scheduler.espa import api_connect

class TestMain(unittest.TestCase):

    @requests_mock.mock()
    def setUp(self, m):
        self.host = "http://localhost:1234"
        self.image = "usgseros/espa-scheduler:latest"
        self.cfg = config()
        
        m.get(self.host, json={"foo": 1})
        self.api = api_connect({"espa_api": self.host, "task_image": self.image})
        self.scheduler = EspaScheduler(self.cfg, self.api)

    def test_get_framework(self):
        framework = get_framework(self.cfg)
        self.assertEqual(list(framework.keys()), ["user", "id", "name", "hostname", "failover_timeout"])


