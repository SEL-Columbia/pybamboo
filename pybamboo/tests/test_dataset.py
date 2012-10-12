from pybamboo.dataset import Dataset
from pybamboo.exceptions import BambooDatasetDoesNotExist,\
    ErrorCreatingBambooDataset
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
