from pybamboo.dataset import Dataset
from pybamboo.exceptions import PyBambooException
from pybamboo.tests.test_base import TestBase


class TestDataset(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self._create_dataset_from_file()

    def _create_dataset_from_file(self):
        self.dataset = Dataset(path=self.CSV_FILE, connection=self.connection)

    def test_create_dataset_no_info(self):
        with self.assertRaises(PyBambooException):
            self.dataset = Dataset()

    def test_create_dataset_from_file(self):
        # created in TestDataset.setUp()
        self.assertTrue(self.dataset.id is not None)

    def test_delete_dataset(self):
        self.dataset.delete()
        self.assertTrue(self.dataset._id is None)

    def test_invalid_dataset(self):
        self.dataset.delete()
        with self.assertRaises(PyBambooException):
            self.dataset.delete()

    def test_add_calculation(self):
        result = self.dataset.add_calculation('double_amount = amount * 2')
        self.assertEqual(result, True)

    def test_add_invalid_calculation_a_priori(self):
        with self.assertRaises(PyBambooException):
            result = self.dataset.add_calculation('just formula')

    def test_add_invalid_calculation_a_posteriori(self):
        result = self.dataset.add_calculation('double_amount = BAD')
        self.assertEqual(result, False)

    def test_remove_calculation(self):
        self.dataset.add_calculation('double_amount = amount * 2')
        result = self.dataset.remove_calculation('double_amount')
        self.assertTrue(result)

    def test_remove_calculation_fail(self):
        result = self.dataset.remove_calculation('bad')
        self.assertFalse(result)
