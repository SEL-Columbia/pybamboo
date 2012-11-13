from pybamboo.dataset import Dataset
from pybamboo.exceptions import PyBambooException
from pybamboo.tests.test_base import TestBase


class TestDataset(TestBase):

    def setUp(self):
        TestBase.setUp(self)
        self._create_dataset_from_file()

    def _create_dataset_from_file(self):
        self.dataset = Dataset(path=self.CSV_FILE, connection=self.connection)

    def test_create_dataset_defualt_connection(self):
        pass

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

    def test_get_calculations(self):
        calc_keys = ['status', 'formula', 'group', 'name']
        result = self.dataset.add_calculation('double_amount = amount * 2')
        self.assertEqual(result, True)
        result = self.dataset.get_calculations()
        self.assertTrue(isinstance(result, list))
        for calc in result:
            self.assertTrue(isinstance(calc, dict))
            keys = calc.keys()
            for key in calc_keys:
                self.assertTrue(key in keys)

    def test_get_summary(self):
        self.wait()  # TODO: remove this (after bamboo fix)
        result = self.dataset.get_summary()
        self.assertTrue(isinstance(result, dict))
        # TODO: assert more stuff?

    def test_get_summary_with_select(self):
        pass

    def test_get_summary_with_groups(self):
        pass

    def test_get_summary_with_query(self):
        pass

    def test_get_summary_with_select_group_and_query(self):
        pass

    def test_get_info(self):
        info_keys = [
            'attribution',
            'description',
            'license',
            'created_at',
            'updated_at',
            'label',
            'num_columns',
            'num_rows',
            'id',
            'schema',
        ]
        schema_keys = [
            'simpletype',
            'olap_type',
            'label',
        ]
        self.wait()  # TODO: remove this (after bamboo fix)
        result = self.dataset.get_info()
        self.assertTrue(isinstance(result, dict))
        for key in info_keys:
            self.assertTrue(key in result.keys())
        self.assertEqual(result['num_columns'], 15)
        self.assertEqual(result['num_rows'], 19)
        schema = result['schema']
        self.assertTrue(isinstance(schema, dict))
        self.assertEqual(len(schema.keys()), 15)
        for col_name, col_info in schema.iteritems():
            for key in schema_keys:
                self.assertTrue(key in col_info.keys())

    def test_get_data(self):
        result = self.dataset.get_data()
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 19)

    def test_get_data_with_select(self):
        pass

    def test_get_data_with_query(self):
        pass

    def test_get_data_with_select_and_query(self):
        pass

    def test_get_no_data(self):
        pass

    def test_update_data(self):
        row = {
            'food_type': 'morning_food',
            'amount': 10.0,
            'risk_factor': 'high_risk',
            'rating': 'delectible',
        }
        self.wait()  # TODO: remove this (after bamboo fix)
        result = self.dataset.update_data([row])
        self.wait()
        result = self.dataset.get_data()
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 20)

    def test_update_data_no_data(self):
        with self.assertRaises(PyBambooException):
            result = self.dataset.update_data([])

    def test_update_data_bad_data(self):
        bad_rows = [
            {},
            [[]],
            [{'exception': Exception()}]
        ]
        for rows in bad_rows:
            with self.assertRaises(PyBambooException):
                result = self.dataset.update_data(rows)
