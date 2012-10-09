from test_base import TestBase
from pybamboo.pybamboo import ErrorParsingBambooData,\
    ErrorRetrievingBambooData, PyBamboo


class TestPyBamboo(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self.fake_id = '31546146'

    def _store_csv(self):
        response = self.pybamboo.store_csv_file(self.CSV_FILE)
        self.dataset_id = response['id']
        return response

    def test_get_url(self):
        self.assertTrue(isinstance(self.pybamboo.get_url(), basestring))

    def test_set_url(self):
        test_url = 'testurl'
        pybamboo = PyBamboo(test_url)
        self.assertEqual(pybamboo.get_url(), test_url)

    def test_get_dataset_url(self):
        self.assertTrue(isinstance(self.pybamboo.get_dataset_url(self.fake_id),
                                   basestring))
        self.assertTrue('datasets' in self.pybamboo.get_dataset_url(
            self.fake_id))

    def test_get_dataset_summary_url(self):
        self.assertTrue(isinstance(self.pybamboo.get_dataset_summary_url(
            self.fake_id), basestring))
        self.assertTrue('summary' in self.pybamboo.get_dataset_summary_url(
            self.fake_id))

    def test_get_dataset_info_url(self):
        self.assertTrue(isinstance(
            self.pybamboo.get_dataset_info_url(self.fake_id), basestring))
        self.assertTrue('info' in self.pybamboo.get_dataset_info_url(
            self.fake_id))

    def test_get_dataset_calculations_url(self):
        self.assertTrue(isinstance(
            self.pybamboo.get_dataset_calculations_url(self.fake_id),
            basestring))
        self.assertTrue(
            'calculations' in self.pybamboo.get_dataset_calculations_url(
                self.fake_id))

    def test_store_csv_file(self):
        response = self._store_csv()
        self.assertTrue('id' in response)

    def test_delete_dataset(self):
        self._store_csv()
        response = self.pybamboo.delete_dataset(self.dataset_id)
        self.assertTrue('success' in response)
        self.dataset_id = None

    def test_store_calculation(self):
        self._store_csv()
        name = 'amount_gps_alt'
        formula = 'amount+gps_alt'
        response = self.pybamboo.store_calculation(
            self.dataset_id, name, formula)
        self.assertTrue('success' in response)
        self.assertTrue(name in response['success'])
        self.assertTrue(self.dataset_id in response['success'])

    def test_count_submissions(self):
        self._store_csv()
        count = self.pybamboo.count_submissions(self.dataset_id, 'amount')
        count = int(count)
        self.assertTrue(isinstance(count, int))
        self.assertEqual(count, self.NUM_ROWS)

    def test_count_submissions_dimension(self):
        self._store_csv()
        count = self.pybamboo.count_submissions(self.dataset_id, 'rating')
        count = int(count)
        self.assertTrue(isinstance(count, int))
        self.assertEqual(count, self.NUM_ROWS)

    def test_count_submissions_mean(self):
        self._store_csv()
        mean = self.pybamboo.count_submissions(
            self.dataset_id, 'amount', 'mean')
        mean = float(mean)
        self.assertTrue(isinstance(mean, float))

    def test_count_submissions_mean_dimension(self):
        self._store_csv()
        mean = self.pybamboo.count_submissions(
            self.dataset_id, 'rating', 'mean')
        mean = float(mean)
        self.assertTrue(isinstance(mean, float))
        self.assertEqual(mean, self.NUM_ROWS)

    def test_query(self):
        self._store_csv()
        response = self.pybamboo.query(self.dataset_id)
        self.assertTrue(isinstance(response, list))
        self.assertEqual(len(response), self.NUM_ROWS)

    def test_query_select(self):
        self._store_csv()
        response = self.pybamboo.query(self.dataset_id, select={'amount': 1})
        self.assertTrue(isinstance(response, list))
        self.assertEqual(len(response), self.NUM_ROWS)

    def test_query_first(self):
        self._store_csv()
        response = self.pybamboo.query(self.dataset_id, first=True)
        self.assertTrue(isinstance(response, dict))
        self.assertEqual(len(response), self.NUM_COLS)

    def test_query_last(self):
        self._store_csv()
        response = self.pybamboo.query(self.dataset_id, last=True)
        self.assertTrue(isinstance(response, dict))
        self.assertEqual(len(response), self.NUM_COLS)

    def test_query_summary(self):
        self._store_csv()
        response = self.pybamboo.query(self.dataset_id, as_summary=True)
        self.assertTrue(isinstance(response, dict))
        self.assertEqual(len(response), self.NUM_COLS)

    def test_bad_url(self):
        self.pybamboo.url = 'http://google.com'
        try:
            self._store_csv()
        except ErrorRetrievingBambooData:
            pass

    def test_bad_response(self):
        self.pybamboo.url = 'http://google.com'
        self.pybamboo.OK_STATUS_CODES = self.pybamboo.OK_STATUS_CODES + (404,)
        try:
            self._store_csv()
        except ErrorParsingBambooData:
            pass

    def test_merge(self):
        self._store_csv()
        dataset_id1 = self.dataset_id
        self._store_csv()
        dataset_id2 = self.dataset_id
        response = self.pybamboo.merge([dataset_id1, dataset_id2])
        self.assertTrue(isinstance(response, dict))
        self.assertTrue('id' in response)
        merge_id = response['id']
        d1_response = self.pybamboo.query(dataset_id1)
        response = self.pybamboo.query(merge_id)
        self.assertEqual(len(d1_response) * 2, len(response))
        # cleanup, tearDown will cleanup dataset_id2
        self.pybamboo.delete_dataset(dataset_id1)
        self.pybamboo.delete_dataset(merge_id)
