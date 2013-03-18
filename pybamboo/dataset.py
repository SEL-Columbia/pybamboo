
import StringIO

from pybamboo.connection import Connection
from pybamboo.decorators import require_valid, retry
from pybamboo.exceptions import PyBambooException
from pybamboo.utils import safe_json_dumps


class Dataset(object):
    """
    Object that represents a dataset in bamboo.
    """

    _id = None
    NA_VALUES = []
    NUM_RETRIES = 3.0
    AGGREGATIONS = [
        'max',
        'mean',
        'min',
        'median',
        'newest',
        'sum',
        'ratio',
        'count',
    ]
    DATA_FORMATS = [
        'csv',
        'json',
    ]

    def __init__(self, dataset_id=None, url=None,
                 path=None, content=None, data_format='csv',
                 schema_path=None, schema_content=None,
                 na_values=None,
                 connection=None):
        """
        Create a new pybamboo.Dataset from one of the following:
            * dataset_id - the id of an existing bamboo.Dataset
            * url - url to a .csv file
            * path - path to a local .csv or .json file
            * content - a CSV or JSON string
            * data_format - whether path or content is csv | json
            * schema_path - path to a JSON SDF schema
            * schema_content - a JSON SDF string

        One can also pass in a pybamboo.Connection object.  If this is not
        supplied one will be created automatically with the default options.
        """
        if dataset_id is None and url is None \
                and path is None and content is None \
                and schema_path is None and schema_content is None:
            raise PyBambooException(
                'Must supply dataset_id, url, content, schema or file path.')

        if data_format not in self.DATA_FORMATS:
            raise PyBambooException('Illegal data_format: %s. data_format'
                                    ' must be one of %s' %
                                    (data_format, self.DATA_FORMATS))

        req_data = {}
        if na_values is not None:
            if not isinstance(na_values, (list, tuple, set)):
                raise PyBambooException('N/A values must be a list.')
            self.NA_VALUES = na_values
            req_data.update({'na_values':
                             safe_json_dumps(na_values,
                                             PyBambooException('na_values '
                                             'are not JSON-serializable'))})

        if connection is None:
            self._connection = Connection()
        else:
            self._connection = connection

        if dataset_id is not None:
            # TODO: check if this dataset exists?
            self._id = dataset_id
            return

        if url is not None:
            # TODO: check valid url?
            req_data.update({'url': url})
            self._id = self._connection.make_api_request(
                'POST', '/datasets', req_data).get('id')
            return

        # files might be overloaded by schema or path/content
        files = {}

        if schema_path is not None or schema_content is not None:
            # TODO: check for bad file stuff?
            schema_data = schema_content if schema_content is not None \
                else open(schema_path)
            files.update({'schema': ('data.schema.json', schema_data)})

        if path is not None or content is not None:
            # TODO: check for bad file stuff?
            data = content if content is not None else open(path)
            files.update({'%s_file' % data_format:
                         ('data.%s' % data_format, data)})

        self._id = self._connection.make_api_request('POST', '/datasets',
                                                     files=files,
                                                     data=req_data).get('id')

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

    def add_calculation(self, name=None, formula=None, groups=None,
                        num_retries=NUM_RETRIES):
        """
        Adds a calculation to this dataset in bamboo.

        A calculation is defined by:
        :param str name: the formula name by which you'll identify it.
        :param str formula: the actual formula using bamboo syntax.
            cf. http://bamboo.io/docs/index.html#formula-reference
        :param list groups: optionnal, a list of fields to group on.

        .. note ::
            http://bamboo.io/docs/basic_commands.html#calculation-formulas
        """
        @require_valid
        @retry(num_retries)
        def _add_calculation(self, formula, name, groups):
            if (formula is None or name is None
                or not isinstance(formula, basestring)
                or not isinstance(name, basestring)):
                raise PyBambooException('name & formula must be strings.')

            data = {'name': name, 'formula': formula}

            if groups is not None:
                if not isinstance(groups, list):
                    raise PyBambooException('group must be a list of strings.')
                data['group'] = ','.join(groups)

            response = self._connection.make_api_request(
                'POST', '/datasets/%s/calculations' % self._id, data=data)
            return 'error' not in response.keys()
        return _add_calculation(self, formula, name, groups)

    def add_calculations(self,
                         path=None,
                         content=None,
                         json=None,
                         num_retries=NUM_RETRIES):
        """
        Adds a JSON-formatted list of calculations to this dataset in bamboo.

        Calculations are defined by a name, a formula and an optionnal grouping
        This method takes a JSON list of dict, each containing:
        * str name
        * str formula
        * list group (optionnal)

        JSON content can be send in 3 ways:
        :param str path: localation of a valid JSON file.
        :param StringIO content: a file-like object containing valid JSON
        :param dict json: a Python list of dict to be serialized.

        .. note ::
            http://bamboo.io/docs/advanced_commands.html
                                  #creating-multiple-calculations-via-json
        """
        @require_valid
        @retry(num_retries)
        def _add_calculations(self, path, content, json):

            files = {}

            if path is None and content is None and json is None:
                raise PyBambooException('JSON fomulae must be provided as '
                                        'JSON, path or content.')

            if json is not None:
                json_data = safe_json_dumps(json,
                                            PyBambooException('formulae are '
                                                              'not JSON-serial'
                                                              'izable'))
                content = StringIO.StringIO(json_data)
            data = content if content is not None else open(path)

            files.update({'json_file': ('data.json', data)})

            response = self._connection.make_api_request(
                'POST', '/datasets/%s/calculations' % self._id, files=files)
            return 'error' not in response.keys()
        return _add_calculations(self, path, content, json)

    def remove_calculation(self, name, num_retries=NUM_RETRIES):
        """
        Removes a calculation from this dataset in bamboo.
        """
        @require_valid
        @retry(num_retries)
        def _remove_calculation(self, name):
            response = self._connection.make_api_request(
                'DELETE', '/datasets/%s/calculations/%s' % (self._id, name))
            return 'success' in response.keys()
        return _remove_calculation(self, name)

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
            'GET', '/datasets/%s/aggregations' % self._id)
        return dict([(group, Dataset(dataset_id, connection=self._connection))
                     for group, dataset_id in response.iteritems()])

    @require_valid
    def get_aggregations(self):
        return self.get_aggregate_datasets()

    def get_summary(self, select='all', groups=None, query=None,
                    order_by=None, limit=0, callback=None,
                    num_retries=NUM_RETRIES):
        """
        Returns the summary information for this dataset.
        """
        @require_valid
        @retry(num_retries)
        def _get_summary(self, select, groups, query, order_by,
                         limit, callback):
            params = {}
            # TODO: check input params
            if select != 'all':
                if not isinstance(select, list):
                    raise PyBambooException(
                        'select must be a list of strings.')
                select = dict([(sel, 1) for sel in select])
                params['select'] = safe_json_dumps(
                    select,
                    PyBambooException('select is not JSON-serializable.'))
            else:
                params['select'] = select
            if groups is not None:
                if not isinstance(groups, list):
                    raise PyBambooException(
                        'groups must be a list of strings.')
                params['group'] = ','.join(groups)
            if query is not None:
                if not isinstance(query, dict):
                    raise PyBambooException('query must be a dict.')
                params['query'] = safe_json_dumps(
                    query,
                    PyBambooException('query is not JSON-serializable.'))
            if order_by:
                if not isinstance(order_by, basestring):
                    raise PyBambooException('order_by must be a string.')
                params['order_by'] = order_by
            if limit:
                if not isinstance(limit, int):
                    raise PyBambooException('limit must be an int.')
                params['limit'] = safe_json_dumps(
                    limit,
                    PyBambooException('limit is not JSON-serializable.'))
            if callback:
                if not isinstance(callback, basestring):
                    raise PyBambooException('callback must be a string.')
                params['callback'] = callback
            return self._connection.make_api_request(
                'GET', '/datasets/%s/summary' % self._id, params=params)
        return _get_summary(self, select, groups, query, order_by,
                            limit, callback)

    def get_info(self, callback=None, num_retries=NUM_RETRIES):
        """
        Returns the general information for this dataset.
        """
        @require_valid
        @retry(num_retries)
        def _get_info(self, callback):
            params = {}
            if callback:
                if not isinstance(callback, basestring):
                    raise PyBambooException('callback must be a string.')
                params['callback'] = callback
            return self._connection.make_api_request(
                'GET', '/datasets/%s/info' % self._id, params=params)
        return _get_info(self, callback)

    def set_info(self, attribution=None, description=None,
                 label=None, license=None,
                 num_retries=NUM_RETRIES):
        """
        Set metadata on the dataset
        """
        @require_valid
        @retry(num_retries)
        def _set_info(self, attribution, description, label, license):
            params = {}
            if attribution is not None:
                if not isinstance(attribution, basestring):
                    raise PyBambooException('attribution must be a string.')
                params['attribution'] = attribution
            if description is not None:
                if not isinstance(description, basestring):
                    raise PyBambooException('description must be a string.')
                params['description'] = description
            if label is not None:
                if not isinstance(label, basestring):
                    raise PyBambooException('label must be a string.')
                params['label'] = label
            if license is not None:
                if not isinstance(license, basestring):
                    raise PyBambooException('license must be a string.')
                params['license'] = license
            return self._connection.make_api_request(
                'PUT', '/datasets/%s/info' % self._id, data=params)
        return _set_info(self, attribution, description, label, license)

    def get_data(self, select=None, query=None, order_by=None, limit=0,
                 distinct=None, format=None, callback=None, count=False,
                 index=False,
                 num_retries=NUM_RETRIES):
        """
        Returns the rows in this dataset filtered by the given
        select and query.
        """
        @require_valid
        def _get_data(self, select, query, order_by, limit, distinct,
                      format, callback, count, index):
            params = {}
            if select:
                if not isinstance(select, list):
                    raise PyBambooException(
                        'select must be a list of strings.')
                select = dict([(sel, 1) for sel in select])
                params['select'] = safe_json_dumps(
                    select,
                    PyBambooException('select is not JSON-serializable.'))
            if query:
                if not isinstance(query, dict):
                    raise PyBambooException('query must be a dict.')
                params['query'] = safe_json_dumps(
                    query,
                    PyBambooException('query is not JSON-serializable.'))
            if order_by:
                if not isinstance(order_by, basestring):
                    raise PyBambooException('order_by must be a string.')
                params['order_by'] = order_by
            if format:
                if not isinstance(format, basestring):
                    raise PyBambooException('format must be a string.')
                params['format'] = format
            if distinct:
                if not isinstance(distinct, basestring):
                    raise PyBambooException('distinct must be a string.')
                params['distinct'] = distinct
            if callback:
                if not isinstance(callback, basestring):
                    raise PyBambooException('callback must be a string.')
                params['callback'] = callback
            if limit:
                if not isinstance(limit, int):
                    raise PyBambooException('limit must be an int.')
                params['limit'] = safe_json_dumps(
                    limit,
                    PyBambooException('limit is not JSON-serializable.'))
            if count:
                params['count'] = bool(count)
            if index:
                params['index'] = bool(index)
            return self._connection.make_api_request(
                'GET', '/datasets/%s' % self._id, params=params)
        return _get_data(self, select, query, order_by, limit, distinct,
                         format, callback, count, index)

    def resample(self, date_column=None, interval=None, how=None,
                 query=None, format=None):
        """
        Returns the rows in this dataset resampled by a date column.

        The parameters are

            date_column: The date column to resample on.
            interval: A code for the interval to use.
                      Any pandas codes are accepted.
                      e.g. 'D' for daily, 'W' for weekly, 'M' for monthly.
                      Complete list: http://pytseries.sourceforge.net
                                          /core.constants.html#date-frequencies
            how: (Optional) How to calculate the grouped samples.
                      e.g: sum, mean, std, max, min, median, first, last, ohlc
                      or any function/numpy array function. default is 'mean'.
            query: (Optional) A MongoDB query to restrict the dataset,
                      to only data matching the query will be resampled.
            format: (Optional) format of the resampled data (CSV/JSON).

        """
        @require_valid
        def _resample(self, date_column, interval, how, query, format):
            params = {}
            if not date_column or not isinstance(date_column, basestring):
                raise PyBambooException('date_column must be a string.')

            params['date_column'] = date_column

            if not interval or not isinstance(interval, basestring):
                raise PyBambooException('interval must be a string '
                                        'representing a frequency. cf. '
                                        'http://pytseries.sourceforge.net/'
                                        'core.constants.html#date-frequencies')
            params['interval'] = interval

            if how:
                if not isinstance(how, basestring):
                    raise PyBambooException('how must be a string.')
                params['how'] = how

            if format:
                if not isinstance(format, basestring):
                    raise PyBambooException('format must be a string.')
                params['format'] = format

            if query:
                if not isinstance(query, dict):
                    raise PyBambooException('query must be a dict.')
                params['query'] = safe_json_dumps(
                    query,
                    PyBambooException('query is not JSON-serializable.'))

            return self._connection.make_api_request(
                'GET', '/datasets/%s/resample' % self._id, params=params)
        return _resample(self, date_column, interval, how, query, format)

    def rolling(self, win_type=None, window=None, format=None):
        """
            To compute moving or rolling statistics / moments

            you can use the rolling request.
            Any options that can be passed to the pandas
            rolling_window function can be passed as parameters to bamboo.
            http://pandas.pydata.org/pandas-docs/dev
                /computation.html#moving-rolling-statistics-moments
            Window types are passed as the win_type parameter.
        """

        @require_valid
        def _rolling(self, win_type, window, format):
            params = {}
            if win_type:
                if not isinstance(win_type, basestring):
                    raise PyBambooException('win_type must be a string.')
                params['win_type'] = win_type

            if not window or not isinstance(window, int):
                raise PyBambooException('window must be an int')
            params['window'] = window

            if format:
                if not isinstance(format, basestring):
                    raise PyBambooException('format must be a string.')
                params['format'] = format

            return self._connection.make_api_request(
                'GET', '/datasets/%s/rolling' % self._id, params=params)
        return _rolling(self, win_type, window, format)

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
        data = {
            'update': safe_json_dumps(rows, PyBambooException(
                'rows is not JSON-serializable'))
        }
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

        data = {'dataset_ids': safe_json_dumps(
            checked_datasets,
            PyBambooException('datasets is not JSON-serializable.'))}
        result = connection.make_api_request(
            'POST', '/datasets/merge', data=data)

        if 'id' in result.keys():
            return Dataset(result['id'], connection=connection)
        # this is never reached...
        # see TestDataset.test_merge_fail()
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

    def count(self, field, method='count'):
        """ Number of rows/submissions for a given field.

        For measure fields method is one of:
        '25%', '50%', '75%', 'count' (default), 'max', 'mean', 'min', 'std' """

        value = self.get_summary().get(field).get('summary')
        if not isinstance(value, dict):
            raise PyBambooException('summary not available.')
        if method in value:
            return float(value.get(method))
        else:
            return sum((int(relval) for relval in value.values()))

    @require_valid
    def row(self, action=None, index=None, payload=None):
        data = None
        if not action in ('show', 'delete', 'edit'):
            raise PyBambooException('Row action must be show|edit|delete.')
        if not isinstance(index, int):
            raise PyBambooException('index must be an int.')
        if action == 'edit':
            data = {"data": safe_json_dumps(payload, PyBambooException(
                    'payload is not JSON-serializable'))}
        http_action = {'show': 'GET',
                       'delete': 'DELETE',
                       'edit': 'PUT'}.get(action)
        return self._connection.make_api_request(
            http_action, '/datasets/%s/row/%d' % (self._id, index), data=data)

    @require_valid
    def delete_row(self, index):
        return self.row(action='delete', index=index)

    @require_valid
    def get_row(self, index):
        return self.row(action='show', index=index)

    @require_valid
    def update_row(self, index, data):
        return self.row(action='edit', index=index, payload=data)

    @property
    def id(self):
        """
        The id of this Dataset in bamboo.
        """
        return self._id

    @property
    def version(self):
        """
        The version of bamboo where this dataset is stored.
        """
        return self._connection.version

    @property
    def columns(self):
        """
        A list of column headers for this dataset.
        """
        cols = self.get_info()['schema'].keys()
        cols.sort()
        return cols

    def __nonzero__(self):
        """
        Returns True if self._id is not None.
        """
        return bool(self._id)

    def __str__(self):
        """
        Returns a string representation of this dataset (id).
        """
        return self._id
