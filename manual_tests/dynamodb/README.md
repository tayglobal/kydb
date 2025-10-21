# DynamoDB Manual Test

This manual test exercises the DynamoDB implementation of KYDB against a local
[DynamoDB Local](https://hub.docker.com/r/amazon/dynamodb-local) instance.

## Prerequisites

* Docker (or another method for running DynamoDB Local)
* Python dependencies installed via `pip install -r requirements.txt`

If Docker is not available, you can emulate the DynamoDB API locally by using
[`moto`](https://docs.getmoto.org/en/stable/docs/server_mode.html)'s server
mode:

```bash
pip install "moto[server]"
MOTO_SERVICE=dynamodb moto_server -p 8000
```

Leave the moto server running in the background while executing the manual test
commands below.

## Running DynamoDB Local

Start a local instance that listens on port 8000:

```bash
docker run --rm -p 8000:8000 amazon/dynamodb-local
```

Leave this container running in a separate terminal while you execute the
manual test script.

## Executing the manual test

From the project root:

```bash
export PYTHONPATH=.
python manual_tests/dynamodb/run_manual_test.py --recreate-table
```

The script will:

1. Ensure placeholder AWS credentials and region configuration are present.
2. Create (or recreate) a table that matches KYDB's DynamoDB schema.
3. Exercise common KYDB operations (writes, reads, listings, directory support,
   deletions) against the local DynamoDB instance.
4. Clean up the test keys.

Use `--drop-table` if you also want to remove the table when the script
finishes.

## Customisation

* `--endpoint-url` changes the DynamoDB endpoint. Defaults to
  `http://localhost:8000`.
* `--table-name` picks a different table name. By default the script reuses the
  `KINYU_UNITTEST_DYNAMODB` environment variable when available.
* `--region` sets the AWS region reported to boto3. DynamoDB Local accepts any
  region value.
