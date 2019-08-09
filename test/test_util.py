import unittest
import re
from mock import patch

from scheduler import util

class TestUtil(unittest.TestCase):
    def setUp(self):
        self.foo = "bar"

    def tearDown(self):
        self.foo = 1

    def test_right_now(self):
        # '08-09-2019:15:41:16.908500'        
        rightnow = util.right_now()
        match = re.match("[0-9]{2}-[0-9]{2}-[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{6}", rightnow)
        self.assertEqual(match.string, rightnow)
