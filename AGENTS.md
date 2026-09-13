# kydb Agent Instructions

## Standard test setup

Run commands from this repository root (`kydb`) and always set
`PYTHONPATH=.`.  The test suite relies on that import path.

The normal test fixture is local-only: `KYDB_TEST_LOCAL_SERVICES` defaults to
`s3,dynamodb,redis` and uses Moto/fakeredis.  This is the appropriate default
for ordinary unit tests.

## Testing against real DynamoDB

Use a real DynamoDB table only when explicitly requested.  The configured
`ak-dev-instance` role does not currently have DynamoDB discovery, query, or
table-creation permissions.  The `kakuto` SSO administrator profile is the
profile that was used successfully for a real test run:

- AWS account: `792811916206`
- Region: `ap-northeast-1` (Japan)
- Sign in when necessary: `AWS_PROFILE=kakuto aws sso login --use-device-code`

Before creating anything, tell the user the proposed account, region, and
table name.  Use a disposable, date-suffixed name such as
`kydb-real-tests-YYYYMMDD`; never point this suite at a production table.

For the normal real-service suite, the table must have:

- a string `path` hash key;
- a string `folder` attribute;
- a numeric `mtime` attribute;
- a `folder-index` global secondary index with `folder` as its hash key and
  an `INCLUDE` projection containing `mtime` and `ctime`;
- a `folder-time-index` global secondary index with `folder` as its hash key,
  `mtime` as its range key, and an `INCLUDE` projection containing `ctime`;
- a numeric `class_date` attribute and a `folder-class_date-index` global
  secondary index with `folder` as its hash key, `class_date` as its range
  key, and an `INCLUDE` projection containing `mtime` and `ctime`.  This is
  the per-business-key index shape (`folder-<name>-index`, `<name>` as a
  Number range key) that `db.set(key, value, index={'<name>': 1})` and
  `db.folder(f).by('<name>')` need; `class_date` is the one the test suite
  uses.  It is additive and sparse: a table without it keeps working, and
  only `by('class_date')` fails until it is added;
- on-demand (`PAY_PER_REQUEST`) billing.

Tag tables at creation with `Purpose=kydb-real-tests` and
`ManagedBy=openclaw`.

Wait for the table to become active before testing:

```bash
AWS_PROFILE=kakuto aws dynamodb wait table-exists \
  --region ap-northeast-1 --table-name "$table_name"
```

Disable the local-service fixture and run only the DynamoDB implementation
cases.  Set the table name explicitly:

```bash
PYTHONPATH=. \
KYDB_TEST_LOCAL_SERVICES='' \
KYDB_TEST_DB_TYPES=dynamodb \
KINYU_UNITTEST_DYNAMODB="$table_name" \
AWS_PROFILE=kakuto \
AWS_DEFAULT_REGION=ap-northeast-1 \
python -m pytest \
  kydb/impl/tests/test_impl.py kydb/impl/tests/test_index_compat.py -vv
```

Add `kydb/impl/tests/test_user_index_dynamodb.py` and
`kydb/tests/test_gym_signups.py` to that list to exercise business keys
against the real table; both need `folder-class_date-index` to exist.  With
`KYDB_TEST_LOCAL_SERVICES=''` the local fixtures start nothing, and
`KYDB_TEST_DB_TYPES=dynamodb` skips the memory and Redis parametrisations.

The completed real-service run on 2026-09-02 reported 64 passed tests and 23
backend-inapplicable skips across `test_impl.py` and
`test_index_compat.py`. Real GSIs are eventually consistent, unlike Moto, so
service-facing assertions use the bounded `assert_eventually_equal` helper.
The run used an isolated temporary Python environment with `pytest`, `boto3`,
and `PyYAML` installed; do not add a virtual environment to this repo just to
run the test.

The separate backfill/1 MiB validation starts from a fresh disposable table
that has only `folder-index`; the test seeds 800 long-key rows, creates
`folder-time-index` while the table is populated, waits for it to become
active, and verifies an unbounded query crosses a real 1 MiB response page.
It mutates the table schema and is therefore gated separately:

```bash
PYTHONPATH=. \
KYDB_TEST_LOCAL_SERVICES='' \
KINYU_UNITTEST_DYNAMODB="$table_name" \
KYDB_RUN_REAL_INDEX_VALIDATION=1 \
AWS_PROFILE=kakuto \
AWS_DEFAULT_REGION=ap-northeast-1 \
python -m pytest kydb/impl/tests/test_real_dynamodb_index.py -vv
```

On 2026-09-02 this test passed in 531.05 seconds. DynamoDB reported
`Backfilling=true`; after the GSI became active, the first query returned a
`LastEvaluatedKey` and all 800 entries were recovered across multiple pages.

## Discovering and cleaning real test databases

Discovery is read-only.  List only tables using the dedicated prefix, then
inspect their status and tags before considering deletion:

```bash
AWS_PROFILE=kakuto aws dynamodb list-tables --region ap-northeast-1 \
  --query "TableNames[?starts_with(@, 'kydb-real-tests-')]" --output text

AWS_PROFILE=kakuto aws dynamodb describe-table --region ap-northeast-1 \
  --table-name "$table_name" \
  --query 'Table.{Status:TableStatus,Arn:TableArn,Items:ItemCount}' --output json

AWS_PROFILE=kakuto aws dynamodb list-tags-of-resource --region ap-northeast-1 \
  --resource-arn "$table_arn" --output json
```

The current test code may leave folder-metadata records behind (for example,
the `test_rmdir_not_empty` cases left seven records in the 2026-09-02 run).
For an isolated table, deleting the table itself is the most reliable cleanup.

By default, delete the temporary real-DynamoDB table at the end of a test run,
after reporting the test result.  Keep it only when the user explicitly asks
to keep it.  Before deletion, verify both its `kydb-real-tests-` name and its
`Purpose=kydb-real-tests` tag.  Then use:

```bash
AWS_PROFILE=kakuto aws dynamodb delete-table --region ap-northeast-1 \
  --table-name "$table_name"
AWS_PROFILE=kakuto aws dynamodb wait table-not-exists --region ap-northeast-1 \
  --table-name "$table_name"
```

Report whether the table was deleted or retained.  Do not delete a table that
was not created for this test run without explicit user confirmation.
