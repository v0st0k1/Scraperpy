import boto3

dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')

table = dynamodb.create_table(
    TableName='using_API',
    KeySchema=[
        {
            'AttributeName': 'Cif',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'Ventas',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'Cif',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'Ventas',
            'AttributeType': 'N'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

table.meta.client.get_waiter('table_exists').wait(TableName='using_API')
