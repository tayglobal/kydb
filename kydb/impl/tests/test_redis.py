import kydb
from unittest.mock import patch
import os


@patch('kydb.impl.redis.RedisDB._get_secret_from_kms', return_value="my-pretend-password")
def test_conneciton_from_config(mock_func):
    os.environ['KYDB_CONFIG_PATH'] = __file__.rsplit(
        os.sep, 1)[0] + '/test_redis_config.yml'
    kms_key = 'abcd1234-a123-456a-a12b-a123b4cd56ef'
    db = kydb.connect("redis://my-redis-db")
    print("Hello")
    print(db._config)
    mock_func.assert_called_once_with('KYDB_REDIS_PASSWORD', kms_key)
    assert True
