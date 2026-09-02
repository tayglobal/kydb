import time
from kydb.base import BaseDB
from boto3.dynamodb.conditions import Key
from kydb.folder_meta import FolderMetaMixin
import boto3


class DynamoDB(FolderMetaMixin, BaseDB):

    def __init__(self, url: str):
        super().__init__(url)
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
        objname = key.rsplit('/', 1)[1]

        # `folder` is a DynamoDB reserved word, so it must be referenced
        # via an ExpressionAttributeNames placeholder rather than literally
        # in the UpdateExpression.
        if FolderMetaMixin._is_folder_meta(objname):
            # Directories are excluded from the folder-time-index: no
            # mtime/ctime is written for `.folder-*` marker records, which
            # keeps that (sparse) GSI free of directories automatically.
            self.table.update_item(
                Key={'path': key},
                UpdateExpression='SET #f=:f, contents=:c',
                ExpressionAttributeNames={'#f': 'folder'},
                ExpressionAttributeValues={':f': folder, ':c': value})
        else:
            now_ns = time.time_ns()
            self.table.update_item(
                Key={'path': key},
                UpdateExpression=(
                    'SET #f=:f, contents=:c, mtime=:t, '
                    'ctime=if_not_exists(ctime, :t)'),
                ExpressionAttributeNames={'#f': 'folder'},
                ExpressionAttributeValues={
                    ':f': folder, ':c': value, ':t': now_ns})

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
