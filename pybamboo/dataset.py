from functools import wraps

from pybamboo.exceptions import BambooDatasetDoesNotExist,\
    ErrorCreatingBambooDataset
from pybamboo.connection import Connection


class Dataset(object):
    """
    Object that represents a dataset in bamboo.
    """

    _id = None

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
            raise ErrorCreatingBambooDataset(
                'Must supply dataset_id, url, or file path.')

        if connection is None:
            self._connection = Connection()

        if dataset_id is not None:
            pass

        if url is not None:
            pass

        if path is not None:
            files = {'csv_file': ('data.csv', open(path))}
            self._id = self._connection.make_api_request(
                'POST', '/datasets', files=files).get('id')

    def require_valid(func):
        """
        This decorator checks whether or not this object corresponds to
        a valid dataset in bamboo.  If not, it raises an exception.
        """
        def wrapped(self, *args, **kwargs):
            if self._id is None:
                raise BambooDatasetDoesNotExist()
            return func(self, *args, **kwargs)
        return wrapped

    @require_valid
    def delete(self):
        """
        Deletes the dataset from bamboo.
        """
        response = self._connection.make_api_request(
            'DELETE', '/datasets/%s' % self._id)
        if 'success' in response.keys():
            self._id = None

    @require_valid
    def add_calculation(self, formula):
        """
        Adds a calculation to this dataset in bamboo.
        """
        pass

    @require_valid
    def remove_calculation(self, name):
        """
        Removes a calculation from this dataset in bamboo.
        """
        pass

    @require_valid
    def get_calculations(self):
        """
        Returns a dict of the calculations in {name: formula} format.
        """
        pass

    @require_valid
    def add_aggregation(self, formula, groups=None):
        """
        Adds an aggregation from this dataset in bamboo the results for which
        are in a dataset which is linked to this one.
        """
        pass

    @require_valid
    def remove_aggregation(self, formula, groups=None):
        """
        Removes this aggregation from the corresponding dataset.
        (Not yet implemented)
        """
        pass

    @require_valid
    def get_aggregations(self):
        """
        Returns a {groups: id} mapping for aggregations of this dataset.
        """
        pass

    @require_valid
    def get_summary(self, select='all', groups=None, query=None):
        """
        Returns the summary information for this dataset.
        """
        pass

    @require_valid
    def get_info(self):
        """
        Returns the general information for this dataset.
        """
        pass

    @require_valid
    def get_data(self, select=None, query=None):
        """
        Returns the rows in this dataset filtered by the given
        select and query.
        """
        pass

    @require_valid
    def update_data(self, rows):
        """
        Updates this dataset with the rows given in {column: value} format.
        Any unspecified columns will result in n/a values.
        """
        pass

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
