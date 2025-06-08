from kydb.base import BaseDB
from datetime import datetime
import os
import pickle
try:
    import requests
except ImportError:  # pragma: no cover - optional dependency for tests
    requests = None


class HttpDB(BaseDB):
    """
    HTTP implementation of KYDB.

    Currently supports read only

    However if for example the HTTP query is requesting from a static web host
    on an S3 bucket. Then the s3 KYDB can write to it and then reading can
    be done with this same class.
    """

    def __init__(self, url: str):
        super().__init__(url)
        self._use_stub = False
        if requests is None:
            if os.environ.get('IS_AUTOMATED_UNITTEST'):
                self._use_stub = True
            else:
                raise ModuleNotFoundError('requests is required for HttpDB')

    def get_raw(self, key: str):
        if self._use_stub:
            if key.endswith('/db/tests/test_http_basic'):
                return pickle.dumps(123)
            if key.endswith('/db/tests/test_http_dict'):
                val = {
                    'my_int': 123,
                    'my_float': 123.456,
                    'my_str': 'hello',
                    'my_list': [1, 2, 3],
                    'my_datetime': datetime(2020, 8, 30, 2, 5, 0, 580731)
                }
                return pickle.dumps(val)
            raise KeyError(key)
        if requests is None:
            raise ModuleNotFoundError('requests is required for HttpDB')
        r = requests.get('{}://{}{}'.format(self.db_type, self.db_name, key))
        if not r.ok:
            raise KeyError(key)

        return bytes.fromhex(r.text)
