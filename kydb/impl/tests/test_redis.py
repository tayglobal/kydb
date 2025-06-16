import kydb
from unittest.mock import patch
import os

FAKE_REDIS_PASSWORD = "my-pretend-password"


def kydb_config_path(func):
    def wrapped(*args, **kwargs):
        kydb_config_path_orig = os.environ.get("KYDB_CONFIG_PATH")
        plain_password = os.environ.get("KYDB_REDIS_PLAIN_PASSWORD")
        try:
            os.environ["KYDB_CONFIG_PATH"] = (
                __file__.rsplit(os.sep, 1)[0] + "/test_redis_config.yml"
            )
            os.environ["KYDB_REDIS_PLAIN_PASSWORD"] = FAKE_REDIS_PASSWORD
            func(*args, **kwargs)
        finally:
            if kydb_config_path_orig is None:
                del os.environ["KYDB_CONFIG_PATH"]
            else:
                os.environ["KYDB_CONFIG_PATH"] = kydb_config_path_orig

            if plain_password is None:
                del os.environ["KYDB_REDIS_PLAIN_PASSWORD"]
            else:
                os.environ["KYDB_REDIS_PLAIN_PASSWORD"] = plain_password

    return wrapped


@patch(
    "kydb.impl.redis.RedisDB._get_secret_from_kms", return_value="my-pretend-password"
)
@kydb_config_path
def test_conneciton_from_config_kms(mock_get_secret):
    kms_key = "abcd1234-a123-456a-a12b-a123b4cd56ef"
    db = kydb.connect("redis://kms-encrypted-redis")
    mock_get_secret.assert_called_once_with("KYDB_REDIS_PASSWORD", kms_key)
    kwargs = db._get_connection_kwargs(db.db_name)
    assert kwargs["host"] == "my-redis-host"
    assert kwargs["port"] == 1234
    assert kwargs["password"] == FAKE_REDIS_PASSWORD


@kydb_config_path
def test_conneciton_from_config_plain():
    db = kydb.connect("redis://plain-redis")
    kwargs = db._get_connection_kwargs(db.db_name)
    print(kwargs)
    assert kwargs["host"] == "my-redis-host2"
    assert kwargs["port"] == 1234
    assert kwargs["password"] == FAKE_REDIS_PASSWORD


def test_real_redis():
    """Only run this if you have a redis environment setup

    You'll need to setup an env var $KYDB_CONFIG_PATH which points to a yaml file with the content
    similar to what's in ``kydb/impl/tests/test_redis_config.yml``

    You can either use ``encryption-method: plain`` or ``encryption-method: kms``

    If using kms encryption,  you'll need to encrypt the password with the code below.

    ```python
    from kydb.impl.redis import RedisDB

    print(RedisDB.encrypt_secret('<your-password>', '<your-kms-key-id>'))
    ```

    Once you have the encrypted password, you can put it in your environment variable and run the test.
    """
    db = kydb.connect("redis://real-redis-db")
    folder = "/test_folder"
    db[f"{folder}/foo"] = 123
    db[f"{folder}/bar"] = 234
    db.clear_cache()
    actual = [db[f"{folder}/{x}"] for x in db.ls(folder)]
    expected = [123, 234]
    assert actual == expected
