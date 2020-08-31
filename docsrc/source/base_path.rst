Base Path
=========


You can set the base path of a KYDB as follows::

    db = kydb.connect('dynamodb://my-source-db/home/tony.yum')
    db['foo'] = 'Hello World!'

Now 'Hello World!' would be written to */home/tony.yum/foo* in the *my-source-db* DynamoDB.

Same applies to any other implmentations.
