import os
import boto3
import pytest
from contextlib import ExitStack


@pytest.fixture(scope="session", autouse=True)
def local_services():
    """Spin up local services for testing if requested.

    The environment variable ``KYDB_TEST_LOCAL_SERVICES`` accepts a comma
    separated list of services (``s3``, ``dynamodb``, ``redis``).  Required
    third-party libraries must be installed otherwise the tests will be
    skipped.
    """
    services = {
        s.strip()
        for s in os.environ.get(
            "KYDB_TEST_LOCAL_SERVICES", "s3,dynamodb,redis"
        ).split(",")
        if s.strip()
    }
    if not services:
        yield
        return

    stack = ExitStack()
    missing = []

    if "s3" in services:
        try:
            from moto import mock_aws
        except ImportError:
            missing.append("moto")
        else:
            m = mock_aws()
            stack.enter_context(m)
            os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
            s3 = boto3.client("s3", region_name="us-east-1")
            bucket = os.environ.get("KINYU_UNITTEST_S3_BUCKET", "kydb-test")
            s3.create_bucket(Bucket=bucket)

    if "dynamodb" in services:
        try:
            from moto import mock_aws
        except ImportError:
            missing.append("moto")
        else:
            m = mock_aws()
            stack.enter_context(m)
            os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
            dynamodb = boto3.client("dynamodb", region_name="us-east-1")
            table = os.environ.get("KINYU_UNITTEST_DYNAMODB", "kydb-test-table")
            dynamodb.create_table(
                TableName=table,
                KeySchema=[{"AttributeName": "path", "KeyType": "HASH"}],
                AttributeDefinitions=[
                    {"AttributeName": "path", "AttributeType": "S"},
                    {"AttributeName": "folder", "AttributeType": "S"},
                ],
                ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
                GlobalSecondaryIndexes=[
                    {
                        "IndexName": "folder-index",
                        "KeySchema": [{"AttributeName": "folder", "KeyType": "HASH"}],
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {
                            "ReadCapacityUnits": 5,
                            "WriteCapacityUnits": 5,
                        },
                    }
                ],
            )

    if "redis" in services:
        try:
            import redis
            import fakeredis
        except ImportError:
            missing.append("fakeredis")
        else:
            server = fakeredis.FakeServer()
            original = redis.Redis

            def fake_constructor(*args, **kwargs):
                return fakeredis.FakeRedis(server=server, *args, **kwargs)

            redis.Redis = fake_constructor

            def restore():
                redis.Redis = original

            stack.callback(restore)

    if missing:
        pytest.skip("Missing local service dependencies: {}".format(", ".join(sorted(set(missing)))))

    with stack:
        yield
