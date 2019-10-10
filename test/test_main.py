import json
import os
import re
import requests
import requests_mock
import unittest

from addict import Dict
from mock import patch
from unittest.mock import Mock
from multiprocessing import Queue

from scheduler.main import ESPAFramework
from scheduler.config import config
from scheduler.espa import api_connect

class TestMain(unittest.TestCase):

    @requests_mock.mock()
    def setUp(self, m):
        self.host = "http://localhost:1234"
        self.image = "usgseros/espa-scheduler:latest"
        self.cfg = config()
        self.cfg['mesos_master'] = "http://127.0.0.1:5050"

        worklist = Queue()
        
        m.get(self.host, json={"foo": 1})
        self.api = api_connect({"espa_api": self.host, "task_image": self.image})
        self.framework = ESPAFramework(self.cfg, self.api, worklist)

    def test__getResource(self):
        resource = Dict()
        resource.name = "cpus"
        resource.scalar.value = 1.0
        resp = self.framework._getResource([resource], "cpus")
        self.assertEqual(resp, 1.0)

    def test__updateResource(self):
        resource = Dict()
        resource.name = "mem"
        resource.scalar.value = 5120
        self.framework._updateResource([resource], "mem", 1024)
        self.assertEqual(resource.scalar.value, 4096)

    def test_accept_offer(self):
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

        self.assertTrue(self.framework.accept_offer(offer_good))
        self.assertFalse(self.framework.accept_offer(offer_bad))

    def test_core_limit_reached(self):
        framework = self.framework
        framework.runningList = ["a", "b", "c"]
        self.assertFalse(framework.core_limit_reached())

        framework.max_cpus = 2
        self.assertTrue(framework.core_limit_reached())

        framework.runningList = []
        framework.max_cpus = 10

    @patch('scheduler.espa.APIServer.mesos_tasks_disabled', lambda i: True)
    def test_offer_received_disabled(self):
        driver = Mock()
        offers = [Mock()]
        resp = self.framework.offer_received(offers)
        self.assertFalse(resp.tasks.enabled)

    @patch('scheduler.espa.APIServer.mesos_tasks_disabled', lambda i: False)
    @patch('scheduler.espa.APIServer.get_products_to_process', lambda a, b, c: {"products": []})
    @patch('scheduler.main.ESPAFramework.accept_offer', lambda a, b: True)
    def test_offer_received_nowork(self):
        driver = Mock()
        offers = [Mock()]

        resp = self.framework.offer_received(offers)
        self.assertTrue(resp.tasks.enabled)

    @patch('scheduler.espa.APIServer.mesos_tasks_disabled', lambda i: False)
    @patch('scheduler.espa.APIServer.get_products_to_process', lambda a, b, c: {"products": [{"orderid": "foo@manchu.com-123", "sceneid": "L8BBCC"}, {"orderid": "foo@manchu.com-123", "sceneid": "L7BBCC"}]})
    @patch('scheduler.espa.APIServer.set_to_scheduled', lambda a, b: True)
    @patch('scheduler.espa.APIServer.update_status', lambda a, b, c, d: True)
    @patch('scheduler.main.ESPAFramework.accept_offer', lambda a, b: True)
    @patch('scheduler.task.build', lambda a, b, c, d, e, f, g, h: {'agent_id': {'value': 'foo'}})
    def test_offer_received_work(self):
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
        offer_good = Mock()
        offer_good.resources = [cpu_resource_good, mem_resource_good, disk_resource_good]

        offers = [offer_good]

        class MockOffer(object):
            def empty(self):
                return False

            def get(self, failfast):
                return {"orderid":"foo", "scene":"bar"}

        self.framework.workList = MockOffer()

        #resp = self.framework.offer_received(offers)
        #self.assertTrue(resp.tasks.enabled)
        #self.assertEqual(resp.offers.accepted, 1)
        

    def test_status_update(self):
        driver = Mock()
        
        update = dict()
        update['status'] = {'task_id': {'value': "orderid_@@@_unitid"}} 
        update['status']['state'] = "TASK_RUNNING"

        resp = self.framework.status_update(update)

        self.assertEqual(resp['status'], "healthy")
        self.assertEqual(resp['task_id'], "orderid_@@@_unitid")
        self.assertEqual(resp['state'], update['status']['state'])
        self.assertEqual(resp['list']['name'], "running")
        self.assertEqual(resp['list']['status'], "new")
