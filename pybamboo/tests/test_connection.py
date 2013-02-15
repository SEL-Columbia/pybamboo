from pybamboo.connection import Connection, DEFAULT_BAMBOO_URL,\
    OK_STATUS_CODES
from pybamboo.exceptions import BambooError, ErrorParsingBambooData,\
    PyBambooException
from pybamboo.tests.test_base import TestBase


class TestConnection(TestBase):

    def test_url(self):
        # default self.connection is setup in TestBase.setUp()
        self.assertEqual(self.connection.url, self.TEST_BAMBOO_URL)
        test_url = 'http://test.com'
        self.connection.url = test_url
        self.assertEqual(self.connection.url, test_url)

    def test_check_response(self):
        test_response = self.MockResponse()
        for status_code in OK_STATUS_CODES:
            test_response.status_code = status_code
            test_response.text = 'OK'
            try:
                self.connection._check_response(test_response)
            except BambooError:  # pragma: no cover
                self.fail('Raised error on OK status code.')
        test_response.status_code = 400
        test_response.text = 'FAIL'
        with self.assertRaises(BambooError):
            self.connection._check_response(test_response)

    def test_version(self):
        self.assert_keys_in_dict(self.VERSION_KEYS,
            self.connection.version)
