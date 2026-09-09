#!/usr/bin/env python3
"""Manual DynamoDB integration test for KYDB.

This script assumes a local DynamoDB endpoint is available (for example via
amazon/dynamodb-local) and exercises the high-level KYDB operations against it.
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable

import boto3
from botocore.exceptions import ClientError

import kydb


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Run a manual KYDB smoke test against a DynamoDB instance."
            " The script will create the requested table if it does not"
            " already exist and then exercise common KYDB operations."
        )
    )
    parser.add_argument(
        "--endpoint-url",
        default=os.environ.get("AWS_ENDPOINT_URL", "http://localhost:8000"),
        help=(
            "Endpoint URL for DynamoDB. The default assumes a local"
            " dynamodb-local container bound to port 8000."
        ),
    )
    parser.add_argument(
        "--table-name",
        default=os.environ.get("KINYU_UNITTEST_DYNAMODB", "kydb-manual-test"),
        help="DynamoDB table name to use for the manual test.",
    )
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_DEFAULT_REGION", "us-west-2"),
        help="AWS region reported to boto3 (arbitrary when using dynamodb-local).",
    )
    parser.add_argument(
        "--recreate-table",
        action="store_true",
        help="Delete the table before running the test so a clean slate is used.",
    )
    parser.add_argument(
        "--drop-table",
        action="store_true",
        help="Drop the table after the test completes successfully.",
    )
    return parser.parse_args()


def _ensure_env(region: str, endpoint_url: str) -> None:
    """Populate environment variables needed by boto3.

    dynamodb-local accepts any credentials, but boto3 still expects values to be
    present. Default placeholders keep the setup frictionless for manual tests.
    """

    os.environ.setdefault("AWS_ACCESS_KEY_ID", "dummy-access-key")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "dummy-secret-key")
    os.environ.setdefault("AWS_DEFAULT_REGION", region)
    if endpoint_url:
        os.environ["AWS_ENDPOINT_URL"] = endpoint_url


def _client(region: str, endpoint_url: str):
    return boto3.client("dynamodb", region_name=region, endpoint_url=endpoint_url)


def _table_exists(client, table_name: str) -> bool:
    tables: Iterable[str] = client.list_tables().get("TableNames", [])
    return table_name in tables


def _delete_table_if_exists(client, table_name: str) -> None:
    try:
        client.delete_table(TableName=table_name)
    except client.exceptions.ResourceNotFoundException:
        return

    waiter = client.get_waiter("table_not_exists")
    waiter.wait(TableName=table_name)


def _create_table(client, table_name: str) -> None:
    try:
        client.create_table(
            TableName=table_name,
            AttributeDefinitions=[
                {"AttributeName": "path", "AttributeType": "S"},
                {"AttributeName": "folder", "AttributeType": "S"},
            ],
            KeySchema=[{"AttributeName": "path", "KeyType": "HASH"}],
            GlobalSecondaryIndexes=[
                {
                    "IndexName": "folder-index",
                    "KeySchema": [
                        {"AttributeName": "folder", "KeyType": "HASH"}
                    ],
                    "Projection": {"ProjectionType": "ALL"},
                    "ProvisionedThroughput": {
                        "ReadCapacityUnits": 5,
                        "WriteCapacityUnits": 5,
                    },
                }
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5,
            },
        )
    except client.exceptions.ResourceInUseException:
        return

    waiter = client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)


def _exercise_kydb(table_name: str) -> None:
    db_url = f"dynamodb://{table_name}/"
    db = kydb.connect(db_url)
    test_root = "/manual_dynamodb_test"
    test_key = f"{test_root}/numbers/one"
    second_key = f"{test_root}/numbers/two"
    complex_key = f"{test_root}/objects/widget"
    empty_dir = f"{test_root}/empty-folder"

    # Clean up any leftovers from a previous run.
    try:
        db.rm_tree(test_root)
    except KeyError:
        pass

    print(f"Writing primitive data to {test_key}")
    db[test_key] = 1
    assert db.exists(test_key), "First write should exist"
    assert db[test_key] == 1, "Stored integer should be retrievable"

    print(f"Writing secondary value to {second_key}")
    db[second_key] = 2
    print(f"Current leaf nodes under {test_root}/numbers: {list(db.list_dir(f'{test_root}/numbers'))}")
    assert sorted(db.list_dir(f"{test_root}/numbers")) == ["one", "two"], (
        "Two primitives should be listed without trailing slashes"
    )

    print(f"Writing complex object to {complex_key}")
    payload = {"value": 42, "items": [1, 2, 3], "flag": True}
    db[complex_key] = payload
    assert db[complex_key] == payload, "Complex payload should round-trip"
    assert db.read(complex_key, reload=True) == payload, "Reload should bypass cache"

    listing = sorted(db.list_dir(test_root))
    print(f"Top-level contents of {test_root}: {listing}")
    assert listing == ["numbers/", "objects/"], (
        "Directory listing should surface folders with trailing slashes"
    )

    print(f"Creating explicit directory at {empty_dir}")
    db.mkdir(empty_dir)
    assert db.is_dir(empty_dir), "mkdir should create a directory marker"
    assert db.ls(empty_dir) == [], "New directory should be empty"

    listing_after_mkdir = sorted(db.list_dir(test_root))
    print(f"Top-level contents after mkdir: {listing_after_mkdir}")
    assert listing_after_mkdir == ["empty-folder/", "numbers/", "objects/"], (
        "mkdir should introduce a new directory entry"
    )

    print(f"Deleting {second_key}")
    db.delete(second_key)
    assert not db.exists(second_key), "delete should remove the target key"

    print(f"Cleaning up {test_root}")
    db.rm_tree(test_root)
    assert not db.is_dir(test_root), "Cleanup should remove the test root"

    print("Manual DynamoDB test finished successfully.")


def main() -> int:
    args = _parse_args()
    _ensure_env(args.region, args.endpoint_url)

    client = _client(args.region, args.endpoint_url)

    if args.recreate_table:
        print(f"Recreating table {args.table_name}")
        _delete_table_if_exists(client, args.table_name)

    if not _table_exists(client, args.table_name):
        print(f"Creating DynamoDB table {args.table_name}")
        _create_table(client, args.table_name)
    else:
        print(f"Reusing existing DynamoDB table {args.table_name}")

    try:
        _exercise_kydb(args.table_name)
    except AssertionError as exc:
        print(f"Manual test failed: {exc}", file=sys.stderr)
        return 1
    except ClientError as exc:
        print(f"AWS client error: {exc}", file=sys.stderr)
        return 1
    finally:
        if args.drop_table:
            print(f"Dropping table {args.table_name}")
            _delete_table_if_exists(client, args.table_name)

    return 0


if __name__ == "__main__":
    sys.exit(main())
