import os
import time
import unittest

import requests

from pybamboo.connection import Connection, DEFAULT_BAMBOO_URL


class TestBase(unittest.TestCase):

    class MockResponse(object):
        pass

    CSV_FILE = os.getcwd() + '/tests/fixtures/good_eats.csv'
    AUX_CSV_FILE = os.getcwd() + '/tests/fixtures/good_eats_aux.csv'
    NUM_COLS = 15
    NUM_ROWS = 19
    TEST_BAMBOO_URL = 'http://localhost:8080'

    def setUp(self):
        self.bamboo_url = self.TEST_BAMBOO_URL
        self.connection = Connection(self.bamboo_url)
        self.dataset = None

    def tearDown(self):
        if self.dataset:
            self._delete_dataset()

    def _delete_dataset(self):
        response = requests.delete(
            self.bamboo_url + '/datasets/%s' % self.dataset.id)

    def wait(self, seconds=5):
        time.sleep(seconds)
