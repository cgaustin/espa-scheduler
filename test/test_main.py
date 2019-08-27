import json
import os
import re
import requests
import requests_mock
import unittest

from addict import Dict
from mock import patch
from unittest.mock import Mock

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

    def test__getResource(self):
        resource = Dict()
        resource.name = "cpus"
        resource.scalar.value = 1.0
        resp = self.scheduler._getResource([resource], "cpus")
        self.assertEqual(resp, 1.0)

    def test__updateResource(self):
        resource = Dict()
        resource.name = "mem"
        resource.scalar.value = 5120
        self.scheduler._updateResource([resource], "mem", 1024)
        self.assertEqual(resource.scalar.value, 4096)

    def test_acceptOffer(self):
        cpu_resource_good  = Dict()
        disk_resource_good = Dict()
        mem_resource_good  = Dict()
        mem_resource_bad   = Dict()
        
        cpu_resource_good.name = "cpus"
        cpu_resource_good.scalar.value = 8

        disk_resource_good.name = "disk"
        disk_resource_good.scalar.value = 10240

        mem_resource_good.name = "mem"
        mem_resource_good.scalar.value = 10240

        mem_resource_bad.name = "mem"
        mem_resource_bad.scalar.value = 1024 # default required 5120

        offer_good = Dict()
        offer_good.resources = [cpu_resource_good, mem_resource_good, disk_resource_good]

        offer_bad = Dict()
        offer_bad.resources = [cpu_resource_good, mem_resource_bad]

        self.assertTrue(self.scheduler.acceptOffer(offer_good))
        self.assertFalse(self.scheduler.acceptOffer(offer_bad))

    def test_core_limit_reached(self):
        scheduler = self.scheduler
        scheduler.runningList = ["a", "b", "c"]
        self.assertFalse(scheduler.core_limit_reached())

        scheduler.max_cpus = 2
        self.assertTrue(scheduler.core_limit_reached())

        scheduler.runningList = []
        scheduler.max_cpus = 10

    @patch('scheduler.espa.APIServer.mesos_tasks_disabled', lambda i: True)
    def test_resourceOffer_disabled(self):
        driver = Mock()
        offers = [Mock()]
        resp = self.scheduler.resourceOffers(driver, offers)
        self.assertFalse(resp.tasks.enabled)

    @patch('scheduler.espa.APIServer.mesos_tasks_disabled', lambda i: False)
    @patch('scheduler.espa.APIServer.get_products_to_process', lambda a, b, c: {"products": []})
    def test_resourceOffer_nowork(self):
        driver = Mock()
        offers = [Mock()]

        resp = self.scheduler.resourceOffers(driver, offers)
        self.assertTrue(resp.tasks.enabled)
        self.assertEqual(resp.work.length, 0)

    @patch('scheduler.espa.APIServer.mesos_tasks_disabled', lambda i: False)
    @patch('scheduler.espa.APIServer.get_products_to_process', lambda a, b, c: {"products": [{"orderid": "foo@manchu.com-123", "sceneid": "L8BBCC"}, {"orderid": "foo@manchu.com-123", "sceneid": "L7BBCC"}]})
    @patch('scheduler.espa.APIServer.set_to_scheduled', lambda a, b: True)
    def test_resourceOffer_work(self):
        driver = Mock()

        cpu_resource_good  = Dict()
        mem_resource_good  = Dict()
        disk_resource_good = Dict()
        cpu_resource_good.name = "cpus"
        cpu_resource_good.scalar.value = 8
        mem_resource_good.name = "mem"
        mem_resource_good.scalar.value = 10240
        disk_resource_good.name = "disk"
        disk_resource_good.scalar.value = 10240
        offer_good = Dict()
        offer_good.resources = [cpu_resource_good, mem_resource_good, disk_resource_good]

        offers = [offer_good]

        resp = self.scheduler.resourceOffers(driver, offers)
        self.assertTrue(resp.tasks.enabled)
        self.assertEqual(resp.work.length, 2)
        self.assertEqual(resp.offers.accepted, 1)


    def test_statusUpdate(self):
        driver = Mock()
        update = Dict()
        update.task_id.value = "orderid_@@@_unitid"
        update.state = "TASK_RUNNING"

        resp = self.scheduler.statusUpdate(driver, update)

        self.assertEqual(resp.status, "healthy")
        self.assertEqual(resp.task_id, "orderid_@@@_unitid")
        self.assertEqual(resp.state, update.state)
        self.assertEqual(resp.list.name, "running")
        self.assertEqual(resp.list.status, "new")
