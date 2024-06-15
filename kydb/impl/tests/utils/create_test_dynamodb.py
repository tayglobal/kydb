import boto3
import os


TABLE_NAME = os.environ.get("KINYU_UNITTEST_DYNAMODB")
dynamodb = boto3.client("dynamodb")


def create_table():
    index_attr = "path"
    folder_attr = "folder"
    folder_index = folder_attr + "-index"

    table_params = {
        "TableName": TABLE_NAME,
        "AttributeDefinitions": [
            {"AttributeName": index_attr, "AttributeType": "S"},
            {"AttributeName": folder_attr, "AttributeType": "S"},
        ],
        "KeySchema": [{"AttributeName": index_attr, "KeyType": "HASH"}],
        "BillingMode": "PAY_PER_REQUEST",
        "GlobalSecondaryIndexes": [
            {
                "IndexName": folder_index,
                "KeySchema": [{"AttributeName": folder_attr, "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
    }

    response = dynamodb.create_table(**table_params)
    print(response)


def delete_table(stage):
    response = dynamodb.delete_table(TableName=TABLE_NAME)
    print(response)


if __name__ == "__main__":
    create_table()
