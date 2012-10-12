import os
import unittest

import requests

from pybamboo.connection import Connection, DEFAULT_BAMBOO_URL


class TestBase(unittest.TestCase):

    CSV_FILE = os.getcwd() + '/tests/fixtures/good_eats.csv'
    NUM_COLS = 15
    NUM_ROWS = 19

    def setUp(self):
        self.connection = Connection()
        self.dataset = None

    def tearDown(self):
        if self.dataset:
            self._delete_dataset()

    def _delete_dataset(self):
        response = requests.delete(
            DEFAULT_BAMBOO_URL + '/datasets/%s' % self.dataset.id)
