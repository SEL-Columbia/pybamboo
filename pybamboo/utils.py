import simplejson as json


def safe_json_loads(string, exception):
    try:
        return json.loads(string)
    except json.JSONDecodeError:
        raise exception


def safe_json_dumps(data, exception):
    try:
        return json.dumps(data)
    except TypeError:
        raise exception
