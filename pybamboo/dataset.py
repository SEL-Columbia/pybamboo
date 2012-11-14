from functools import wraps
import math
import time

from pybamboo.connection import Connection
from pybamboo.decorators import require_valid, retry
from pybamboo.exceptions import PyBambooException
from pybamboo.utils import safe_json_dumps


class Dataset(object):
    """
    Object that represents a dataset in bamboo.
    """

    _id = None
    NUM_RETRIES = 3.0
    AGGREGATIONS = [
        'max',
        'mean',
        'min',
        'median',
        'sum',
        'ratio',
        'count',
    ]

    def __init__(self, dataset_id=None, url=None, path=None, connection=None):
        """
        Create a new pybamboo.Dataset from one of the following:
            * dataset_id - the id of an existing bamboo.Dataset
            * url - url to a .csv file
            * path - path to a local .csv file

        One can also pass in a pybamboo.Connection object.  If this is not
        supplied one will be created automatically with the default options.
        """
        if dataset_id is None and url is None and path is None:
            raise PyBambooException(
                'Must supply dataset_id, url, or file path.')

        if connection is None:
            self._connection = Connection()
        else:
            self._connection = connection

        if dataset_id is not None:
            # TODO: check if this dataset exists?
            self._id = dataset_id

        if url is not None:
            pass

        if path is not None:
            files = {'csv_file': ('data.csv', open(path))}
            self._id = self._connection.make_api_request(
                'POST', '/datasets', files=files).get('id')

    def _split_formula(self, formula):
        return [token.strip() for token in formula.split('=', 1)]

    def _is_formula_an_aggregation(self, formula):
        for aggregation in self.AGGREGATIONS:
            if formula.startswith(aggregation):
                return True
        return False

    def _process_formula(self, formula, is_aggregation=False):
        try:
            name, formula = self._split_formula(formula)
            if not is_aggregation:
                if self._is_formula_an_aggregation(formula):
                    raise PyBambooException('"%s" is an aggregation. '
                                            'Use Dataset.add_aggregation() '
                                            'instead.' % formula)
            else:
                if not self._is_formula_an_aggregation(formula):
                    raise PyBambooException('"%s" is a calculation. '
                                            'Use Dataset.add_calculation() '
                                            'instead.' % formula)
            return name, formula
        except ValueError:
            raise PyBambooException(
                'Formulas must be of the form: "name = function".')
        except AttributeError:
            raise PyBambooException(
                'Formulas must be expressed as strings.')

    def delete(self, num_retries=NUM_RETRIES):
        """
        Deletes the dataset from bamboo.
        """
        @require_valid
        @retry(num_retries)
        def _delete(self):
            response = self._connection.make_api_request(
                'DELETE', '/datasets/%s' % self._id)
            success = 'success' in response.keys()
            if success:
                self._id = None
            return success
        return _delete(self)

    def add_calculation(self, formula, num_retries=NUM_RETRIES):
        """
        Adds a calculation to this dataset in bamboo.
        """
        @require_valid
        @retry(num_retries)
        def _add_calculation(self, formula):
            name, formula = self._process_formula(formula)
            data = {'name': name, 'formula': formula}
            response = self._connection.make_api_request(
                'POST', '/datasets/%s/calculations' % self._id, data=data)
            return 'error' not in response.keys()
        return _add_calculation(self, formula)

    def add_aggregation(self, formula, groups=None, num_retries=NUM_RETRIES):
        """
        Adds an aggregation to this dataset in bamboo.
        """
        @require_valid
        @retry(num_retries)
        def _add_aggregation(self, formula, groups):
            name, formula = self._process_formula(formula,
                                                  is_aggregation=True)
            data = {'name': name, 'formula': formula}
            response = self._connection.make_api_request(
                'POST', '/datasets/%s/calculations' % self._id, data=data)
            return 'error' not in response.keys()
        return _add_aggregation(self, formula, groups)

    def remove_calculation(self, name, num_retries=NUM_RETRIES):
        """
        Removes a calculation from this dataset in bamboo.
        """
        @require_valid
        @retry(num_retries)
        def _remove_calculation(self, name):
            params = {'name': name}
            response = self._connection.make_api_request(
                'DELETE', '/datasets/%s/calculations' % self._id,
                params=params)
            return 'success' in response.keys()
        return _remove_calculation(self, name)

    def remove_aggregation(self, name, num_retries=NUM_RETRIES):
        """
        Removes an aggregation from this dataset in bamboo.
        """
        return self.remove_calculation(name, num_retries)

    @require_valid
    def get_calculations(self):
        """
        Returns a list of the calculations with their name, formula and group.
        """
        return self._connection.make_api_request(
            'GET', '/datasets/%s/calculations' % self._id)

    @require_valid
    def get_aggregate_datasets(self):
        """
        Returns the aggregate datasets for this dataset in a dictionary
        of the form: {group: dataset, ...}.
        """
        response = self._connection.make_api_request(
            'GET', '/datasets/%s/related' % self._id)
        return dict([(group, Dataset(dataset_id, connection=self._connection))
                     for group, dataset_id in response.iteritems()])

    def get_summary(self, select='all', groups=None, query=None,
                    num_retries=NUM_RETRIES):
        """
        Returns the summary information for this dataset.
        """
        @require_valid
        @retry(num_retries)
        def _get_summary(self, select, groups, query):
            params = {'select': select}
            if groups:
                params['group'] = groups
            if query:
                params['query'] = query
            return self._connection.make_api_request(
                'GET', '/datasets/%s/summary' % self._id, params=params)
        return _get_summary(self, select, groups, query)

    def get_info(self, num_retries=NUM_RETRIES):
        """
        Returns the general information for this dataset.
        """
        @require_valid
        @retry(num_retries)
        def _get_info(self):
            return self._connection.make_api_request(
                'GET', '/datasets/%s/info' % self._id)
        return _get_info(self)

    def get_data(self, select=None, query=None, num_retries=NUM_RETRIES):
        """
        Returns the rows in this dataset filtered by the given
        select and query.
        """
        @require_valid
        @retry(num_retries)
        def _get_data(self, select, query):
            params = {}
            if select:
                params['select'] = select
            if query:
                params['query'] = query
            return self._connection.make_api_request(
                'GET', '/datasets/%s' % self._id, params=params)
        return _get_data(self, select, query)

    @require_valid
    def update_data(self, rows):
        """
        Updates this dataset with the rows given in {column: value} format.
        Any unspecified columns will result in n/a values.
        """
        if not isinstance(rows, list):
            raise PyBambooException(
                'rows must be a list of dictionaries')
        if len(rows) == 0:
            raise PyBambooException(
                'rows must contain at least one row dictionary')
        for row in rows:
            if not isinstance(row, dict):
                raise PyBambooException(
                    'rows must be a list of dictionaries')
        data = safe_json_dumps(rows, PyBambooException(
            'rows is not JSON-serializable'))
        response = self._connection.make_api_request(
            'PUT', '/datasets/%s' % self._id, data=data)
        return 'id' in response.keys()

    @classmethod
    def merge(cls, datasets, connection=None):
        """
        Create a new dataset that is a row-wise merge of those in *datasets*.
        Returns the new merged dataset.
        """
        if connection is None:
            connection = Connection()

        # TODO: allow list of dataset_ids?
        checked_datasets = []
        for dataset in datasets:
            if not isinstance(dataset, Dataset):
                raise PyBambooException(
                    'Datasets need to be instances of Dataset.')
            checked_datasets.append(dataset.id)

        data = {'datasets': safe_json_dumps(
            checked_datasets,
            PyBambooException('datasets is not JSON-serializable.'))}
        result = connection.make_api_request(
            'POST', '/datasets/merge', data=data)

        if 'id' in result.keys():
            return Dataset(result['id'], connection=connection)
        return False

    @classmethod
    def join(cls, left_dataset, right_dataset, on, connection=None):
        """
        Create a new dataset that is the result of a join, where this
        left_dataset is the lefthand side and *right_dataset* is the
        righthand side and *on* is the column on which to join.
        The column that is joined on must be unique in the righthand side
        and must exist in both datasets.
        """
        if connection is None:
            connection = Connection()

        if not isinstance(left_dataset, Dataset) or\
                not isinstance(right_dataset, Dataset):
            raise PyBambooException(
                'datasets must be an instances of Dataset.')

        data = {
            'dataset_id': left_dataset.id,
            'other_dataset_id': right_dataset.id,
            'on': on,
        }
        result = connection.make_api_request(
            'POST', '/datasets/join', data=data)

        if 'id' in result.keys():
            return Dataset(result['id'], connection=connection)
        return False

    @property
    def id(self):
        """
        The id of this Dataset in bamboo.
        """
        return self._id

    def __nonzero__(self):
        """
        Returns True if self._id is not None.
        """
        return bool(self._id)
