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

