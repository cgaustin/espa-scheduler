import unittest
import re
from mock import patch
from addict import Dict

from scheduler import task

class TestTask(unittest.TestCase):
    def setUp(self):
        self.foo = "bar"

    def tearDown(self):
        self.foo = 1

    def test_env_vars(self):
        cfg = {"espa_user": "user",
               "espa_group": "group",
               "espa_storage": "/espa-storage",
               "espa_api": "http://127.0.0.1:9876",
               "espa_work_dir": "/mnt/mesos/sandbox",
               "aster_ged_server_name": "http://127.0.0.1:8888",
               "aux_dir": "/espa-aux",
               "urs_machine": "urs",
               "urs_login": "uname",
               "urs_password": "1234"}

        envvars = task.env_vars(cfg)

        expected = [{"name":"ESPA_USER",             "value":"user"},
                    {"name":"ESPA_GROUP",            "value": "group"},
                    {"name":"ESPA_STORAGE",          "value":"/espa-storage"},
                    {"name":"ESPA_API",              "value":"http://127.0.0.1:9876"},
                    {"name":"ESPA_WORK_DIR",         "value":"/mnt/mesos/sandbox"},
                    {"name":"ASTER_GED_SERVER_NAME", "value":"http://127.0.0.1:8888"},
                    {"name":"AUX_DIR",               "value":"/espa-aux"},
                    {"name":"URS_MACHINE",           "value":"urs"},
                    {"name":"URS_LOGIN",             "value":"uname"},
                    {"name":"URS_PASSWORD",          "value":"1234"}]

        self.assertEqual(envvars, expected)

    def test_volumes(self):
        cfg = {"auxiliary_mount": "/usr/local/aux",
               "aux_dir": "/espa-aux",
               "storage_mount": "/usr/local/storage",
               "espa_storage": "/espa-storage"}

        vols = task.volumes(cfg)
        expected = [{"container_path": "/espa-aux", "host_path": "/usr/local/aux", "mode": "RW"},
                    {"container_path": "/espa-storage", "host_path": "/usr/local/storage", "mode": "RW"}]

        self.assertEqual(vols, expected)

    def test_resources(self):
        resources = task.resources(1, 5120, 10240)
        expected = [{'name':'cpus', 'type':'SCALAR', 'scalar':{'value': 1}},
                    {'name':'mem' , 'type':'SCALAR', 'scalar':{'value': 5120}},
                    {'name':'disk', 'type':'SCALAR', 'scalar':{'value': 10240}}]
        self.assertEqual(resources[0].keys(), expected[0].keys())
        self.assertEqual(len(resources), len(expected))

    def test_command(self):
        work_json = {"foo": 1}
        command = task.command(work_json)
        expected = 'python /src/processing/main.py \'[{"foo":1}]\';exit'
        self.assertEqual(command, expected)

    def test_build(self):
        offer = Dict()
        offer.agent_id.value = "999"
        image_name = "usgseros/espa-worker:latest"
        cpu = 1
        mem = 5120
        disk = 10240
        work = {"foo": 1}
        cfg = {"espa_user": "user",
               "espa_group": "group",
               "espa_storage": "/espa-storage",
               "espa_api": "http://127.0.0.1:9876",
               "espa_work_dir": "/mnt/mesos/sandbox",
               "aster_ged_server_name": "http://127.0.0.1:8888",
               "aux_dir": "/espa-aux",
               "auxiliary_mount": "/usr/local/aux",
               "storage_mount": "/usr/local/storage",
               "urs_machine": "urs",
               "urs_login": "uname",
               "urs_password": "1234"}

        build = task.build("orderid_scenename", offer, image_name, cpu, mem, disk, work, cfg)

        expected = Dict()
        expected.task_id.value = "orderid_scenename"
        expected.agent_id.value = "999"
        expected.name = "task orderid_scenename"
        expected.container.type = "DOCKER"
        expected.container.docker.image = "usgseros/espa-worker:latest"
        expected.container.volumes = [{"container_path": "/espa-aux", "host_path": "/usr/local/aux", "mode": "RW"},
                                      {"container_path": "/espa-storage", "host_path": "/usr/local/storage", "mode": "RW"}]
        expected.resources = [{'name':'cpus', 'type':'SCALAR', 'scalar':{'value': 1}},
                              {'name':'mem' , 'type':'SCALAR', 'scalar':{'value': 5120}},
                              {'name':'disk', 'type':'SCALAR', 'scalar':{'value': 10240}}]
        expected.command.value = "python /src/processing/main.py '[{\"foo\":1}]';exit"
        expected.command.environment.variables = [{"name":"ESPA_USER",             "value":"user"},
                                                  {"name":"ESPA_GROUP",            "value":"group"},
                                                  {"name":"ESPA_STORAGE",          "value":"/espa-storage"},
                                                  {"name":"ESPA_API",              "value":"http://127.0.0.1:9876"},
                                                  {"name":"ESPA_WORK_DIR",         "value":"/mnt/mesos/sandbox"},
                                                  {"name":"ASTER_GED_SERVER_NAME", "value":"http://127.0.0.1:8888"},
                                                  {"name":"AUX_DIR",               "value":"/espa-aux"},
                                                  {"name":"URS_MACHINE",           "value":"urs"},
                                                  {"name":"URS_LOGIN",             "value":"uname"},
                                                  {"name":"URS_PASSWORD",          "value":"1234"}]

        self.assertEqual(build, expected)


