"""Opt-in checks that require a disposable real DynamoDB table.

These checks are intentionally excluded from the normal Moto suite. The
target table must start with only ``folder-index``; this module seeds more
than 1 MiB of projected index keys, adds ``folder-time-index`` to the
populated table, observes the backfill, and verifies real multi-page query
pagination.
"""
import os
import time

import boto3
from boto3.dynamodb.conditions import Key
import pytest


RUN_REAL = os.environ.get('KYDB_RUN_REAL_INDEX_VALIDATION') == '1'
TABLE_NAME = os.environ.get('KINYU_UNITTEST_DYNAMODB')
TIME_INDEX = 'folder-time-index'

pytestmark = pytest.mark.skipif(
    not RUN_REAL,
    reason='set KYDB_RUN_REAL_INDEX_VALIDATION=1 for destructive real test')


def _index_description(client):
    table = client.describe_table(TableName=TABLE_NAME)['Table']
    return next(
        (index for index in table.get('GlobalSecondaryIndexes', [])
         if index['IndexName'] == TIME_INDEX),
        None)


def test_real_gsi_backfill_and_one_megabyte_pagination():
    assert TABLE_NAME, 'KINYU_UNITTEST_DYNAMODB must name a disposable table'
    assert TABLE_NAME.startswith('kydb-real-tests-'), (
        'refusing to mutate a table outside the kydb-real-tests-* namespace')

    client = boto3.client('dynamodb')
    table = boto3.resource('dynamodb').Table(TABLE_NAME)
    description = client.describe_table(TableName=TABLE_NAME)['Table']
    tags = {
        tag['Key']: tag['Value']
        for tag in client.list_tags_of_resource(
            ResourceArn=description['TableArn'])['Tags']
    }
    assert tags.get('Purpose') == 'kydb-real-tests'
    assert tags.get('ManagedBy') == 'openclaw'
    assert _index_description(client) is None, (
        f'{TABLE_NAME} already has {TIME_INDEX}; use a fresh disposable table')

    folder = '/unittests/real-index-pagination/'
    total_items = 800
    timestamp = time.time_ns()

    # A long table key is projected into the GSI. Eight hundred ~1.7 KiB
    # keys guarantee that an unbounded Query crosses DynamoDB's real 1 MiB
    # response boundary while keeping the stored payload tiny.
    with table.batch_writer() as batch:
        for number in range(total_items):
            name = f'{number:04d}-' + ('x' * 1700)
            batch.put_item(Item={
                'path': folder + name,
                'folder': folder,
                'contents': b'x',
                'mtime': timestamp + number,
                'ctime': timestamp + number,
            })

    started = time.monotonic()
    client.update_table(
        TableName=TABLE_NAME,
        AttributeDefinitions=[
            {'AttributeName': 'path', 'AttributeType': 'S'},
            {'AttributeName': 'folder', 'AttributeType': 'S'},
            {'AttributeName': 'mtime', 'AttributeType': 'N'},
        ],
        GlobalSecondaryIndexUpdates=[{
            'Create': {
                'IndexName': TIME_INDEX,
                'KeySchema': [
                    {'AttributeName': 'folder', 'KeyType': 'HASH'},
                    {'AttributeName': 'mtime', 'KeyType': 'RANGE'},
                ],
                'Projection': {
                    'ProjectionType': 'INCLUDE',
                    'NonKeyAttributes': ['ctime'],
                },
            },
        }])

    saw_backfilling = False
    while True:
        description = _index_description(client)
        assert description is not None
        saw_backfilling = saw_backfilling or bool(
            description.get('Backfilling'))
        if description['IndexStatus'] == 'ACTIVE':
            break
        if time.monotonic() - started > 600:
            pytest.fail(
                f'{TIME_INDEX} did not become ACTIVE within 10 minutes')
        time.sleep(1)

    first = table.query(
        IndexName=TIME_INDEX,
        KeyConditionExpression=Key('folder').eq(folder),
        ScanIndexForward=False)
    assert 'LastEvaluatedKey' in first, (
        'the first real query did not cross DynamoDB\'s 1 MiB page boundary')

    pages = 1
    paths = {item['path'] for item in first['Items']}
    start_key = first['LastEvaluatedKey']
    while start_key:
        response = table.query(
            IndexName=TIME_INDEX,
            KeyConditionExpression=Key('folder').eq(folder),
            ScanIndexForward=False,
            ExclusiveStartKey=start_key)
        pages += 1
        paths.update(item['path'] for item in response['Items'])
        start_key = response.get('LastEvaluatedKey')

    assert saw_backfilling, 'real DynamoDB never reported Backfilling=true'
    assert pages >= 2
    assert len(paths) == total_items
