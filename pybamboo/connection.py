import simplejson as json

import requests

from pybamboo.exceptions import ErrorParsingBambooData,\
    ErrorRetrievingBambooData


DEFAULT_BAMBOO_URL = 'http://bamboo.io'
OK_STATUS_CODES = (200, 201, 202)


class Connection(object):
    """
    Object that defines a connection to a bamboo instance.
    """

    def __init__(self, url=DEFAULT_BAMBOO_URL):
        self._url = url

    @property
    def url(self):
        return self._url

    @url.setter
    def url(self, url):
        self._url = url

    def make_api_request(self, http_method, url, data=None, files=None):
        http_function = {
            'GET': requests.get,
            'POST': requests.post,
            'PUT': requests.put,
            'DELETE': requests.delete,
        }
        response = http_function[http_method](
            self.url + url, data=data, files=files)
        self._check_response(response)
        return self._safe_json_loads(response)

    def _check_response(self, response):
        if not response.status_code in OK_STATUS_CODES:
            raise ErrorRetrievingBambooData(u'%d Status Code received.'
                                            % response.status_code)

    def _safe_json_loads(self, response):
        try:
            return json.loads(response.text)
        except json.JSONDecodeError:
            raise ErrorParsingBambooData
