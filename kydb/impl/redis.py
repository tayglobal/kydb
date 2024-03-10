from kydb.base import BaseDB
from kydb.folder_meta import FolderMetaMixin
from redis.exceptions import ResponseError
import redis
import boto3
import os
import base64


class RedisDB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)

        self.connection = redis.Redis(
            **self._get_connection_kwargs(self.db_name))

    def _get_connection_kwargs(self, db_name: str):
        if self._config:
            password = self._get_password()
            host = self._config['host']
            port = self._config['port']
            return {
                'host': host,
                'port': port,
                'password': password,
            }

        return self._get_connection_from_dbname(db_name)

    def _get_password(self):
        pwd_cfg = self._config['password']
        encrypt_method = pwd_cfg['encryption-method']
        if encrypt_method == 'kms':
            return self._get_secret_from_kms(pwd_cfg['env_var'], pwd_cfg['encryption-key'])

        if encrypt_method == 'plain':
            return os.environ[pwd_cfg['env_var']]

        raise ValueError(f'Unknown encryption method: {encrypt_method}')

    @staticmethod
    def _get_secret_from_kms(name, kms_key_id: str):
        kms = boto3.client('kms')
        encrypted = os.environ[name]
        res = kms.decrypt(
            KeyId=kms_key_id,
            CiphertextBlob=base64.b64decode(encrypted))

        return res['Plaintext'].decode()

    @staticmethod
    def encrypt_secret(secret: str, kms_key_id: str):
        kms = boto3.client('kms')
        res = kms.encrypt(
            KeyId=kms_key_id,
            Plaintext=secret)
        return base64.b64encode(res['CiphertextBlob'])

    @staticmethod
    def _get_connection_from_dbname(db_name: str):
        if ':' in db_name:
            host, port = db_name.split(':')
            kwargs = {
                'host': host,
                'port': int(port, 10)
            }
        else:
            kwargs = {
                'host': db_name
            }

        return kwargs

    def get_raw(self, key: str):
        try:
            res = self.connection.get(key)
        except ResponseError:
            raise KeyError(f'{key} is not a valid key')

        if not res:
            raise KeyError(key)

        return res

    def folder_meta_set_raw(self, key: str, value):
        folder, obj = key.rsplit('/', 1)
        self.connection.hset(folder, obj, '.')
        self.connection.set(key, value)

    def delete_raw(self, key: str):
        self.connection.delete(key)
        folder, obj = key.rsplit('/', 1)
        self.connection.hdel(folder, obj)

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)[:-1]
        try:
            for key in self.connection.hgetall(folder).keys():
                yield key.decode()
        except ResponseError:
            raise KeyError(f'{folder} is not a valid folder')
