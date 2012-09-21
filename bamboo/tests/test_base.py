import os
import unittest

from bamboo.bamboo import Bamboo


class TestBase(unittest.TestCase):

    CSV_FILE = os.getcwd() + '/tests/fixtures/good_eats.csv'
    NUM_COLS = 15
    NUM_ROWS = 19

    def setUp(self):
        self.bamboo = Bamboo()
        self.dataset_id = None

    def tearDown(self):
        if self.dataset_id:
            self._delete_dataset()

    def _delete_dataset(self):
        self.bamboo.delete_dataset(self.dataset_id)
