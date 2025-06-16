Developer
=========

This page is for developers of Kinyu Demo.

Style Guide
-----------

`PEP8 <https://www.python.org/dev/peps/pep-0008/>`_ Please.

Use your favouite IDE plugin or at least use `autopep8 <https://github.com/hhatto/autopep8>`_.

If you want to be fancy add Git pre-commit hooks to ensure formatting.


Unittest
--------

Please ensure all unittests passes before PR.

Note that unittests require certain dbs to be setup.
You would need to set the following environment variables:

 * ``KINYU_UNITTEST_S3_BUCKET``: name of the S3 Bucket
 
 * ``KINYU_UNITTEST_DYNAMODB``: name of the DynamoDB
 
 * ``KINYU_UNITTEST_REDIS_HOST``: host of the redis server. Can also end with :port if non standard port. i.e. redis-host:8765

To limit the database types tested you can set ``KYDB_TEST_DB_TYPES`` with
a comma separated list such as ``dynamodb`` or ``s3,redis``.  When running
locally without real infrastructure you can spin up in-memory services by
setting ``KYDB_TEST_LOCAL_SERVICES`` with the services to mock
(``s3``, ``dynamodb``, ``redis``).  Using local services requires the
extra dependencies ``moto`` and ``fakeredis``.

To run the tests under Codex or CI you should execute them from the project
root::

    export PYTHONPATH=.
    IS_AUTOMATED_UNITTEST=1 pytest kydb


