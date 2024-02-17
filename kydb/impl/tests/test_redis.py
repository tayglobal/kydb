import kydb
from unittest.mock import patch
from unittest import skipIf
import os

@patch(
    "kydb.impl.redis.RedisDB._get_secret_from_kms", return_value="my-pretend-password"
)
def test_conneciton_from_config(mock_get_secret):
    kydb_config_path_orig = os.environ.get("KYDB_CONFIG_PATH")
    try:
        os.environ["KYDB_CONFIG_PATH"] = (
            __file__.rsplit(os.sep, 1)[0] + "/test_redis_config.yml"
        )
        kms_key = "abcd1234-a123-456a-a12b-a123b4cd56ef"
        db = kydb.connect("redis://my-redis-db")
        mock_get_secret.assert_called_once_with("KYDB_REDIS_PASSWORD", kms_key)
        kwargs = db._get_connection_kwargs(db.db_name)
        assert kwargs["host"] == "my-redis-host"
        assert kwargs["port"] == 1234
        assert kwargs["password"] == "my-pretend-password"
    finally:
        if kydb_config_path_orig is None:
            del os.environ["KYDB_CONFIG_PATH"]
        else:
            os.environ["KYDB_CONFIG_PATH"] = kydb_config_path_orig


@skipIf(
    "IS_AUTOMATED_UNITTEST" in os.environ, "Automated test does not have redit setup"
)
def test_redis():
    """Only run this if you have a redis environment setup

        You'll need to setup an env var $KYDB_CONFIG_PATH which points to a yaml file with the content
        similar to what's in ``kydb/impl/tests/test_redis_config.yml``

        To encrypt the password, you can use the following code:

    ```python
    from kydb.impl.redis import RedisDB

    print(RedisDB.encrypt_secret('<your-password>', '<your-kms-key-id>'))
    ```
        Once you have the encrypted password, you can put it in your environment variable and run the test.
    """
    db = kydb.connect("redis://real-redis-db")
    db["/test_key"] = "test-value"
    db.clear_cache()
    assert db["/test_key"] == "test-value"
