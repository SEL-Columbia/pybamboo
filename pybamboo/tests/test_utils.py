from pybamboo.utils import safe_json_loads, safe_json_dumps
from pybamboo.tests.test_base import TestBase


class TestUtils(TestBase):

    def test_safe_json_loads(self):
        valid_json = '{"key": "value"}'
        invalid_json = '{"key": "value",}'
        try:
            safe_json_loads(valid_json, Exception)
        except Exception:  # pragma: no cover
            self.fail('Raised error on valid JSON.')
        with self.assertRaises(Exception):
            safe_json_loads(invalid_json, Exception)

    def test_safe_json_dumps(self):
        pass
