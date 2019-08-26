import json
import os
import re
import requests
import requests_mock
import unittest

from unittest.mock import Mock
from mock import patch

from scheduler.espa import api_connect, APIServer, APIException

class TestEspa(unittest.TestCase):

    def setUp(self):
        self.host = "http://localhost:1234"
        self.image = "usgseros/espa-scheduler:latest"
        self.api = APIServer(self.host, self.image)

    @requests_mock.mock()
    def test_request(self, m):
        # no resource
        m.get(self.host, json={"foo": "bar"})
        resp = self.api.request('get')
        self.assertEqual(({"foo": "bar"}, 200), resp)

        # no slash
        m.get("{}/foo".format(self.host), json={"frodo": "baggins"})
        resp = self.api.request('get', resource="foo")
        self.assertEqual(({"frodo": "baggins"}, 200), resp)

        # slash
        m.get("{}/foo".format(self.host), json={"bilbo": "baggins"})
        resp = self.api.request('get', resource="/foo")
        self.assertEqual(({"bilbo": "baggins"}, 200), resp)

        # unexpected status should throw an exception
        with self.assertRaises(Exception):
            m.get("{}/foo".format(self.host), json={"frodo": "baggins"})
            self.api.request('get', resource="foo", status=900)
       
    def raiseRequestException(*args): # this could be done better
        raise requests.RequestException()
     
    @patch('requests.request', raiseRequestException)
    def test_request_apiexception(self):       
        with self.assertRaises(APIException):
            self.api.request('get')

    @requests_mock.mock()
    def test_get_configuration(self, m):
        m.get("{}/configuration/{}".format(self.host, "mesos_master"), json={"mesos_master": "127.0.0.1:999"})
        resp = self.api.get_configuration("mesos_master")
        self.assertEqual(resp, "127.0.0.1:999")

    @requests_mock.mock()
    def test_update_status(self, m):
        m.post("{}/update_status".format(self.host), json={"foo": 1})
        resp = self.api.update_status("L71234EDC", "espa-frodo@shire.com-1234", "error")
        self.assertEqual(list(resp.keys()), ["response", "status", "data"])
        self.assertEqual(resp['data'], {"name": "L71234EDC", "orderid": "espa-frodo@shire.com-1234", "processing_loc": self.image, "status": "error"})

    @requests_mock.mock()
    def test_set_to_scheduled(self, m):
        m.post("{}/update_status".format(self.host), json={"foo": 1})
        unit = {"scene": "L7ABC", "orderid": "espa-bilbo@baggins.come-123"}
        resp = self.api.set_to_scheduled(unit)
        self.assertTrue(resp)

    @requests_mock.mock()
    def test_set_scene_error(self, m):
        m.post("{}/set_product_error".format(self.host), json={"foo": 1})
        resp = self.api.set_scene_error("L71234EDC", "espa-frodo@shire.com-1234", 'error')
        self.assertEqual(list(resp.keys()), ["response", "status", "data"])
        self.assertEqual(resp['data'], {"name": "L71234EDC", "orderid": "espa-frodo@shire.com-1234", "processing_loc": self.image, "error": '"error"'})

    @requests_mock.mock()
    def test_get_products_to_process(self, m):
        m.get("{}/products".format(self.host), json={"foo": 1})
        resp = self.api.get_products_to_process(["landsat"], 50)
        self.assertEqual(list(resp.keys()), ["products", "url"])
        self.assertEqual(resp["url"], "/products?record_limit=50&product_types=['landsat']")

    @requests_mock.mock()
    def test_mesos_tasks_disabled(self, m):
        m.get("{}/configuration/run_mesos_tasks".format(self.host), json={"run_mesos_tasks": 'True'})
        resp = self.api.mesos_tasks_disabled()
        self.assertEqual(resp, False)

    def test__unexpected_status(self):
        with self.assertRaises(Exception):
            self.api._unexpected_status('301', "http://bilbo.net/ard")

    @requests_mock.mock()
    def test_test_connection(self, m):
        m.get(self.host, json={"foo": 1})
        resp = self.api.test_connection()
        self.assertTrue(resp)

    @requests_mock.mock()
    def test_api_connect(self, m):
        m.get(self.host, json={"foo": 1})
        api = api_connect({"espa_api": self.host, "task_image": self.image})
        self.assertEqual(api.base, self.host)
        self.assertEqual(api.image, self.image)
