import boto3

dynamodb = boto3.resource('dynamodb', region_name='eu-west-3')

table = dynamodb.create_table(
    TableName='info_empresasFinal',
    KeySchema=[
        {
            'AttributeName': 'empresa_id',
            'KeyType': 'HASH'
        },
        {
            'AttributeName': 'ventas',
            'KeyType': 'RANGE'
        }
    ],
    AttributeDefinitions=[
        {
            'AttributeName': 'empresa_id',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'ventas',
            'AttributeType': 'N'
        }
    ],
    ProvisionedThroughput={
        'ReadCapacityUnits': 10,
        'WriteCapacityUnits': 10
    }
)

table.meta.client.get_waiter('table_exists').wait(TableName='info_empresasFinal')
