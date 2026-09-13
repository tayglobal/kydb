""" Local service fixtures for the tests that live outside
``kydb/impl/tests``.

``kydb/impl/tests/conftest.py`` starts Moto and fakeredis for the
implementation suite, but a ``conftest.py`` only reaches the directory it
sits in -- so a test *here* that wants a real backend has to ask for one.
:func:`local_backends` is that request, and it is deliberately opt-in
rather than autouse: most tests in this directory are pure in-memory and
have no business paying for a mocked AWS.

It has to compose with that fixture in both directions, because a
full-suite run may reach it with the implementation suite's own fixture
already up, or well before it:

* ``mock_aws()`` nests -- a second one shares the first's backends rather
  than starting a fresh set -- so the table may already exist. It is
  created only when a describe says otherwise.
* ``redis.Redis`` may already have been swapped for a fakeredis
  constructor. Re-patching it with a *different* fake server would not
  break anything, but it would be a second empty Redis that the
  already-cached ``RedisDB`` connections (``kydb.api._db_cache``) do not
  point at, so the existing patch is left alone when one is there.

Hence ``scope='module'`` rather than ``'session'``. Moto resets its
backends when the *outermost* mock stops, so a module-scoped mock that
opened the outermost one (this directory collected first) tears the table
down again on the way out, leaving the implementation suite to build its
own exactly as it would have. A session-scoped one would still be holding
the table open when that suite tried to create it.
"""
import os

import pytest

TABLE_NAME = os.environ.get('KINYU_UNITTEST_DYNAMODB', 'kydb-test-table')

#: The table a kydb DynamoDB database needs, including the per-user-index
#: GSI from ``user_index_plan.md`` section 9. ``folder-class_date-index``
#: is what makes ``by('class_date')`` a single indexed query rather than
#: a scan; it is sparse, so an object written with no ``class_date``
#: never appears in it.
TABLE_KWARGS = {
    'TableName': TABLE_NAME,
    'KeySchema': [{'AttributeName': 'path', 'KeyType': 'HASH'}],
    'AttributeDefinitions': [
        {'AttributeName': 'path', 'AttributeType': 'S'},
        {'AttributeName': 'folder', 'AttributeType': 'S'},
        {'AttributeName': 'mtime', 'AttributeType': 'N'},
        {'AttributeName': 'class_date', 'AttributeType': 'N'},
    ],
    'ProvisionedThroughput': {
        'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
    'GlobalSecondaryIndexes': [
        {
            'IndexName': 'folder-index',
            'KeySchema': [{'AttributeName': 'folder', 'KeyType': 'HASH'}],
            'Projection': {
                'ProjectionType': 'INCLUDE',
                'NonKeyAttributes': ['mtime', 'ctime'],
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
        },
        {
            'IndexName': 'folder-time-index',
            'KeySchema': [
                {'AttributeName': 'folder', 'KeyType': 'HASH'},
                {'AttributeName': 'mtime', 'KeyType': 'RANGE'},
            ],
            'Projection': {
                'ProjectionType': 'INCLUDE',
                'NonKeyAttributes': ['ctime'],
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
        },
        {
            'IndexName': 'folder-class_date-index',
            'KeySchema': [
                {'AttributeName': 'folder', 'KeyType': 'HASH'},
                {'AttributeName': 'class_date', 'KeyType': 'RANGE'},
            ],
            'Projection': {
                'ProjectionType': 'INCLUDE',
                'NonKeyAttributes': ['mtime', 'ctime'],
            },
            'ProvisionedThroughput': {
                'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5},
        },
    ],
}


def _services():
    return {
        s.strip()
        for s in os.environ.get(
            'KYDB_TEST_LOCAL_SERVICES', 's3,dynamodb,redis').split(',')
        if s.strip()
    }


def _start_dynamodb(stack):
    import boto3
    from moto import mock_aws

    stack.enter_context(mock_aws())
    os.environ.setdefault('AWS_DEFAULT_REGION', 'us-east-1')
    client = boto3.client('dynamodb', region_name='us-east-1')
    try:
        client.describe_table(TableName=TABLE_NAME)
    except client.exceptions.ResourceNotFoundException:
        client.create_table(**TABLE_KWARGS)


def _start_redis(stack):
    import redis
    import fakeredis

    if not isinstance(redis.Redis, type):
        # Already swapped for a fakeredis constructor by the
        # implementation suite's fixture -- see the module docstring.
        return

    server = fakeredis.FakeServer()
    original = redis.Redis

    def fake_constructor(*args, **kwargs):
        return fakeredis.FakeRedis(server=server, *args, **kwargs)

    redis.Redis = fake_constructor
    stack.callback(lambda: setattr(redis, 'Redis', original))


@pytest.fixture(scope='module')
def local_backends():
    """ Moto DynamoDB (with the kydb table) and fakeredis, on request.

    Skips rather than fails when the optional test dependencies are
    absent, matching the implementation suite: these are emulators for
    convenience, and a machine without them should not turn a green
    suite red.
    """
    from contextlib import ExitStack

    services = _services()
    missing = []

    with ExitStack() as stack:
        if 'dynamodb' in services:
            try:
                _start_dynamodb(stack)
            except ImportError:
                missing.append('moto')

        if 'redis' in services:
            try:
                _start_redis(stack)
            except ImportError:
                missing.append('fakeredis')

        if missing:
            pytest.skip('Missing local service dependencies: '
                        + ', '.join(sorted(set(missing))))

        yield
