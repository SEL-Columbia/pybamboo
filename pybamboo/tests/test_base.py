import os
import unittest

import requests

from pybamboo.connection import Connection, DEFAULT_BAMBOO_URL


class TestBase(unittest.TestCase):

    CSV_FILE = os.getcwd() + '/tests/fixtures/good_eats.csv'
    NUM_COLS = 15
    NUM_ROWS = 19
    TEST_BAMBOO_URL = DEFAULT_BAMBOO_URL

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
