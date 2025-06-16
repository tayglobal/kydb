import kydb
from datetime import datetime
import pytest
import pickle
import threading
from http.server import ThreadingHTTPServer, SimpleHTTPRequestHandler
from functools import partial

@pytest.fixture
def db(tmp_path):
    base = tmp_path / "site"
    data_dir = base / "db" / "tests"
    data_dir.mkdir(parents=True)

    with open(data_dir / "test_http_basic", "w") as f:
        f.write(pickle.dumps(123).hex())

    val = {
        "my_int": 123,
        "my_float": 123.456,
        "my_str": "hello",
        "my_list": [1, 2, 3],
        "my_datetime": datetime(2020, 8, 30, 2, 5, 0, 580731),
    }

    with open(data_dir / "test_http_dict", "w") as f:
        f.write(pickle.dumps(val).hex())

    class QuietHandler(SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
            pass

    handler = partial(QuietHandler, directory=str(base))
    server = ThreadingHTTPServer(("localhost", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://localhost:{server.server_port}"
    db = kydb.connect(url)
    try:
        yield db
    finally:
        server.shutdown()
        thread.join()


def test_http_basic(db):
    key = '/db/tests/test_http_basic'
    assert db[key] == 123
    assert db.exists(key)


def test_http_not_exist(db):
    assert not db.exists('does_not_exist')


def test_http_dict(db):
    key = '/db/tests/test_http_dict'
    val = {
        'my_int': 123,
        'my_float': 123.456,
        'my_str': 'hello',
        'my_list': [1, 2, 3],
        'my_datetime': datetime(2020, 8, 30, 2, 5, 0, 580731)
    }
    assert db[key] == val
