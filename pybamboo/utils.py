import simplejson as json
from bson import json_util


def safe_json_loads(string, exception):
    try:
        return json.loads(string, object_hook=json_util.object_hook)
    except json.JSONDecodeError:
        raise exception


def safe_json_dumps(data, exception):
    try:
        return json.dumps(data)
    except TypeError:
        raise exception
