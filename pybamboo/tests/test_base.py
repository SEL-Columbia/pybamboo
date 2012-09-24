import os
import unittest

from pybamboo.pybamboo import PyBamboo


class TestBase(unittest.TestCase):

    CSV_FILE = os.getcwd() + '/tests/fixtures/good_eats.csv'
    NUM_COLS = 15
    NUM_ROWS = 19

    def setUp(self):
        self.pybamboo = PyBamboo()
        self.dataset_id = None

    def tearDown(self):
        if self.dataset_id:
            self._delete_dataset()

    def _delete_dataset(self):
        self.pybamboo.delete_dataset(self.dataset_id)
