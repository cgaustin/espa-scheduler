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
        cfg = {"espa_storage": "/espa-storage",
               "espa_api": "http://127.0.0.1:9876",
               "aster_ged_server_name": "http://127.0.0.1:8888",
               "aux_dir": "/espa-aux"}

        envvars = task.env_vars(cfg)

        expected = [{"name":"ESPA_STORAGE",          "value":"/espa-storage"},
                    {"name":"ESPA_API",              "value":"http://127.0.0.1:9876"},
                    {"name":"ASTER_GED_SERVER_NAME", "value":"http://127.0.0.1:8888"},
                    {"name":"AUX_DIR",               "value":"/espa-aux"}]

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
        resources = task.resources(1, 5120)
        expected = [{'name':'cpus', 'type':'SCALAR', 'scalar':{'value': 1}},
                    {'name':'mem' , 'type':'SCALAR', 'scalar':{'value': 5120}}]
        self.assertEqual(resources, expected)

    def test_command(self):
        work_json = {"foo": 1}
        command = task.command(work_json)
        expected = 'python /processing/main.py \'[{"foo":1}]\''
        self.assertEqual(command, expected)

    def test_build(self):
        offer = Dict()
        offer.agent_id.value = "999"
        image_name = "usgseros/espa-worker:latest"
        cpu = 1
        mem = 5120
        work = {"foo": 1}
        cfg = {"espa_storage": "/espa-storage",
               "espa_api": "http://127.0.0.1:9876",
               "aster_ged_server_name": "http://127.0.0.1:8888",
               "aux_dir": "/espa-aux",
               "auxiliary_mount": "/usr/local/aux",
               "storage_mount": "/usr/local/storage"} 

        build = task.build("orderid_scenename", offer, image_name, cpu, mem, work, cfg)

        expected = Dict()
        expected.task_id.value = "orderid_scenename"
        expected.agent_id.value = "999"
        expected.name = "task orderid_scenename"
        expected.container.type = "DOCKER"
        expected.container.docker.image = "usgseros/espa-worker:latest"
        expected.container.volumes = [{"container_path": "/espa-aux", "host_path": "/usr/local/aux", "mode": "RW"},
                                      {"container_path": "/espa-storage", "host_path": "/usr/local/storage", "mode": "RW"}]
        expected.resources = [{'name':'cpus', 'type':'SCALAR', 'scalar':{'value': 1}},
                              {'name':'mem' , 'type':'SCALAR', 'scalar':{'value': 5120}}]
        expected.command.value = "python /processing/main.py '[{\"foo\":1}]'"
        expected.command.environment.variables = [{"name":"ESPA_STORAGE",          "value":"/espa-storage"},
                                                  {"name":"ESPA_API",              "value":"http://127.0.0.1:9876"},
                                                  {"name":"ASTER_GED_SERVER_NAME", "value":"http://127.0.0.1:8888"},
                                                  {"name":"AUX_DIR",               "value":"/espa-aux"}]

        self.assertEqual(build, expected)


