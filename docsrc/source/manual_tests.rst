Manual Tests
============

This section describes how to run the manual DynamoDB test harness that ships
with KYDB. The harness spins up a local DynamoDB instance, provisions the table
schema expected by KYDB, and runs a series of smoke tests that exercise the core
API against the DynamoDB implementation.

DynamoDB
--------

1. Launch `DynamoDB Local <https://hub.docker.com/r/amazon/dynamodb-local>`_
   in a separate terminal:

   .. code-block:: bash

      docker run --rm -p 8000:8000 amazon/dynamodb-local

   If you do not have Docker available, you can emulate DynamoDB by running the
   `moto <https://docs.getmoto.org/en/stable/docs/server_mode.html>`_ test
   server instead:

   .. code-block:: bash

      pip install "moto[server]"
      MOTO_SERVICE=dynamodb moto_server -p 8000

   Leave the local endpoint (either DynamoDB Local or moto) running while
   executing the manual test.

2. Install dependencies if you have not already done so:

   .. code-block:: bash

      pip install -r requirements.txt

3. Execute the manual test script from the repository root:

   .. code-block:: bash

      export PYTHONPATH=.
      python manual_tests/dynamodb/run_manual_test.py --recreate-table

   The script will ensure placeholder AWS credentials exist, create the table
   with KYDB's required primary key and ``folder-index`` global secondary index,
   and run a set of write, read, list, directory and delete operations. At the
   end of the run the temporary data is removed again so repeated runs start from
   a clean state.

Optional flags:

``--endpoint-url``
    Override the DynamoDB endpoint. Defaults to ``http://localhost:8000`` which
    matches the default DynamoDB Local container binding.

``--table-name``
    Name of the table to create or reuse. By default the script will reuse the
    value from ``KINYU_UNITTEST_DYNAMODB`` when the environment variable is set
    or fall back to ``kydb-manual-test``.

``--drop-table``
    Remove the table after the assertions complete successfully. This is useful
    when you only need the table for the manual test run.
