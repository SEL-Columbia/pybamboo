from pybamboo.dataset import Dataset
from pybamboo.exceptions import BambooDatasetDoesNotExist,\
    ErrorCreatingBambooDataset, InvalidBambooCalculation
from pybamboo.tests.test_base import TestBase


class TestDataset(TestBase):

    def _create_dataset_from_file(self):
        self.dataset = Dataset(path=self.CSV_FILE)

    def test_create_dataset_no_info(self):
        with self.assertRaises(ErrorCreatingBambooDataset):
            self.dataset = Dataset()

    def test_create_dataset_from_file(self):
        self._create_dataset_from_file()
        self.assertTrue(self.dataset.id is not None)

    def test_delete_dataset(self):
        self.dataset = Dataset(path=self.CSV_FILE)
        self.dataset.delete()
        self.assertTrue(self.dataset._id is None)

    def test_invalid_dataset(self):
        self._create_dataset_from_file()
        self.dataset.delete()
        with self.assertRaises(BambooDatasetDoesNotExist):
            self.dataset.delete()

    def test_add_calculation(self):
        self._create_dataset_from_file()
        result = self.dataset.add_calculation('double_amount = amount * 2')
        self.assertTrue(result)

    def test_add_calculation_fail(self):
        self._create_dataset_from_file()
        # no name (lack of equals sign)
        with self.assertRaises(InvalidBambooCalculation):
            result = self.dataset.add_calculation('amount * 2')
        # bad formula
        result = self.dataset.add_calculation('double_amount = bad')
        self.assertFalse(result)
