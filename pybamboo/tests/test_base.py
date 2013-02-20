import os
import time
import unittest

import requests

from pybamboo.connection import Connection, DEFAULT_BAMBOO_URL


class TestBase(unittest.TestCase):

    class MockResponse(object):
        pass

    CSV_FILE = os.getcwd() + '/tests/fixtures/good_eats.csv'
    JSON_FILE = os.getcwd() + '/tests/fixtures/good_eats.json'
    AUX_CSV_FILE = os.getcwd() + '/tests/fixtures/good_eats_aux.csv'
    NUM_COLS = 15
    NUM_ROWS = 19
    TEST_BAMBOO_URL = DEFAULT_BAMBOO_URL
    VERSION_KEYS = [
        'version',
        'description',
        'branch',
        'commit',
    ]

    def setUp(self):
        self.bamboo_url = self.TEST_BAMBOO_URL
        self.connection = Connection(self.bamboo_url)

        # these two datasets (if created) will automatically
        # get deleted by the test harness
        # NOTE: do not reuse these names for tests, they
        # should only be created through the helper functions
        self.dataset = None
        self.aux_dataset = None

        # add any additional datasets should be added
        # to this list and they will be deleted as well
        self.datasets_to_delete = []

    def tearDown(self):
        self._delete_datasets()

    def _cleanup(self, dataset):
        self.wait(3)  # give some time
        self.datasets_to_delete.append(dataset)

    def _delete_dataset(self, dataset):
        if hasattr(dataset, 'has_aggs_to_remove')\
                and dataset.has_aggs_to_remove:
            self.wait(3)  # wait for them to finish
            aggs = dataset.get_aggregate_datasets()
            for group, agg in aggs.iteritems():
                agg.delete()
        dataset.delete()

    def _delete_datasets(self):
        if self.dataset:
            self._delete_dataset(self.dataset)
        if self.aux_dataset:
            self._delete_dataset(self.aux_dataset)
        for dataset in self.datasets_to_delete:
            self._delete_dataset(dataset)

    def assert_keys_in_dict(self, keys, d):
        d_keys = d.keys()
        for k in keys:
            self.assertTrue(k in d_keys)

    def wait(self, seconds=5):
        time.sleep(seconds)
