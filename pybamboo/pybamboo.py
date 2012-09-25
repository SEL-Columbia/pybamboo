import simplejson as json
import requests


class ErrorRetrievingBambooData(IOError):
    pass


class ErrorParsingBambooData(ValueError):
    pass


DEFAULT_BAMBOO_URL = 'http://bamboo.io'


class PyBamboo(object):
    
    OK_STATUS_CODES = (200, 201, 202)

    def __init__(self, bamboo_url=DEFAULT_BAMBOO_URL):
        self._bamboo_url = bamboo_url

    def get_url(self):
        return self._bamboo_url

    def set_url(self, url):
        self._bamboo_url = url

    url = property(get_url, set_url)

    def get_dataset_url(self, dataset_id):
        data = {'bamboo_url': self.get_url(),
                'dataset': dataset_id}
        return u'%(bamboo_url)s/datasets/%(dataset)s' % data

    def get_dataset_summary_url(self, dataset_id):
        data = {'dataset_url': self.get_dataset_url(dataset_id),
                'select': 'all'}
        return u'%(dataset_url)s/summary?select=%(select)s' % data

    def get_dataset_info_url(self, dataset_id):
        data = {'dataset_url': self.get_dataset_url(dataset_id)}
        return u'%(dataset_url)s/info' % data

    def get_dataset_calculations_url(self, dataset_id):
        data = {'bamboo_url': self.get_url(),
                'dataset': dataset_id}
        return u'%(bamboo_url)s/calculations/%(dataset)s' % data

    def count_submissions(self, dataset_id, field, method='count'):
        """
        Number of submissions for a given field.

        For measure fields method is one of:
            '25%', '50%', '75%', 'count' (default), 'max', 'mean', 'min', 'std'
        """

        url = self.get_dataset_summary_url(dataset_id)
        req = requests.get(url)
        self._check_response(req, (200, 202))

        response = self._safe_json_loads(req)

        value = response.get(field).get('summary')
        if method in value:
            return float(value.get(method))
        else:
            return sum([int(relval) for relval in value.values()])

    def query(self, dataset_id, select=None, query=None, group=None,
              as_summary=False, first=False, last=False):
        params = {
            'select': select,
            'query': query,
            'group': group
        }

        # remove key with no value
        for key, value in params.items():
            if not value:
                params.pop(key)
            else:
                if isinstance(value, dict):
                    params[key] = json.dumps(value)

        if as_summary:
            url = self.get_dataset_summary_url(dataset_id)
        else:
            url = self.get_dataset_url(dataset_id)

        req = requests.get(url, params=params)

        self._check_response(req, (200, 202))

        response = self._safe_json_loads(req)

        if last:
            return response[-1]
        elif first:
            return response[0]
        else:
            return response

    def store_calculation(self, dataset_id, formula_name, formula):
        url = self.get_dataset_calculations_url(dataset_id)

        data = {'name': formula_name,
                'formula': formula}

        req = requests.post(url, data=data)

        self._check_response(req)

        return self._safe_json_loads(req)

    def store_csv_file(self, csv_file_str):
        files = {'csv_file': ('data.csv', open(csv_file_str))}
        req = requests.post('%s/datasets' % self.url, files=files)
        self._check_response(req)
        return self._safe_json_loads(req)

    def delete_dataset(self, dataset_id):
        req = requests.delete(self.get_dataset_url(dataset_id))
        self._check_response(req)
        return json.loads(req.text)

    def _check_response(self, req, ok_status_codes=None):
        if ok_status_codes is None:
            ok_status_codes = self.OK_STATUS_CODES
        if not req.status_code in ok_status_codes:
            raise ErrorRetrievingBambooData(u"%d Status Code received."
                                            % req.status_code)
    def _safe_json_loads(self, req):
        try:
            return json.loads(req.text)
        except:
            raise ErrorParsingBambooData
