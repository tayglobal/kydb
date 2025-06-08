from kydb.base import BaseDB
import os
try:
    from boto3.dynamodb.conditions import Key
    import boto3
except ImportError:  # pragma: no cover - optional dependency for tests
    boto3 = None
    class Key:
        def __init__(self, *args, **kwargs):
            raise ModuleNotFoundError('boto3 is required for DynamoDB')
from kydb.folder_meta import FolderMetaMixin


class DynamoDB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)
        if boto3 is None:
            if os.environ.get('IS_AUTOMATED_UNITTEST'):
                self.table = None
                return
            raise ModuleNotFoundError('boto3 is required for DynamoDB')
        dynamodb = boto3.resource('dynamodb')
        self.table = dynamodb.Table(self.db_name)

    def get_raw(self, key):
        items = self.table.query(
            KeyConditionExpression=Key('path').eq(key))['Items']

        if not items:
            raise KeyError(key)

        return items[0]['contents'].value

    def folder_meta_set_raw(self, key: str, value):
        folder = key.rsplit('/', 1)[0] + '/'
        self.table.put_item(Item={
            'path': key,
            'folder': folder,
            'contents': value
        })

    def delete_raw(self, key: str):
        self.table.delete_item(Key={
            'path': key,
        })

    def list_dir_meta_folder(self, folder: str, page_size: int):
        folder = self._ensure_slashes(folder)

        done = False
        start_key = None
        while not done:
            kwargs = {
                'IndexName': "folder-index",
                'KeyConditionExpression': Key('folder').eq(folder),
                'Limit': page_size
            }

            if start_key:
                kwargs['ExclusiveStartKey'] = start_key

            res = self.table.query(**kwargs)

            for item in res.get('Items', []):
                yield item['path'].rsplit('/', 1)[1]

            start_key = res.get('LastEvaluatedKey', None)
            done = start_key is None
