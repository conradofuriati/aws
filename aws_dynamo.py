
import os
import json
import uuid
import boto3
import pandas as pd
from decimal import Decimal
from functools import reduce
from boto3.dynamodb.conditions import Key, And, Attr

os.chdir('C:\\test')

#create table
def create_table(name, schema, attributes, provisioned):
    dynamodb = boto3.resource('dynamodb')
    response = dynamodb.create_table(TableName=name, KeySchema=schema, AttributeDefinitions=attributes, ProvisionedThroughput=provisioned)
    return response

name = 'table_name'
schema = [{'AttributeName': 'Id', 'KeyType': 'S'}, {'AttributeName': 'Payload', 'KeyType': 'S'}] #nao podem ser alterados
attributes = [{'AttributeName': 'Id', 'KeyType': 'S'}, {'AttributeName': 'Payload', 'KeyType': 'S'}]
provisioned = {'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}

create_table(name, schema, attributes, provisioned)


#create item
def create_item(table, body):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    tbl.put_item(Item=body)

table = 'table_name'
body = {'Id': '2', 'Payload': "{'user': '1', 'product': 'tenis', 'amount': '199.50'}"}

create_item(table, body)


#update item [erro: corrigir key tbl (retirar payload)]
def update_item(table, key, expression, attributes):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    tbl.update_item(Key=key, UpdateExpression=expression, ExpressionAttributeValues=attributes)

table = 'table_name'
key = {'Id': '2', 'Payload': "{'user': '1', 'product': 'tenis', 'amount': '199.50'}"}
expression = 'SET payload = :val'
attributes = {':val': "{'user': '2', 'origem': 'aliexpress', 'product': 'tenis', 'amount': '299.50'}"}

update_item(table, key, expression, attributes)


#delete item
def delete_item(table, key):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    tbl.delete_item(Key=key)

table = 'table_name'
key = {'Id': '2', 'Payload': "{'user': '2', 'origem': 'aliexpress', 'product': 'tenis', 'amount': '299.50'}"}

delete_item(table, key)


###
#query w/ index
def query_table(table, index, key, value):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    response = tbl.query(IndexName=index, KeyConditionExpression=Key(key).eq(value))
    df = pd.DataFrame(response['Items'])
    return df

table = 'table_name'
index = 'Column-index'
key = 'column_name'
value = '2080000'

df = query_table(table, index, key, value)


#query without index
def query_table(table, index, key, value):
    if index != None:
        dynamodb = boto3.resource('dynamodb')
        tbl = dynamodb.Table(table)
        response = tbl.query(IndexName=index, KeyConditionExpression=Key(key).eq(value))
        df = pd.DataFrame(response['Items'])
        return df
    else:
        dynamodb = boto3.resource('dynamodb')
        tbl = dynamodb.Table(table)
        response = tbl.query(KeyConditionExpression=Key(key).eq(value))
        df = pd.DataFrame(response['Items'])
        return df

table = 'table_name'
index = None
key = 'column_name'
value = '2080000'

df = query_table(table, index, key, value)


#query multiple conditions
def query_multiple(table, key, value, key2, value2):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    response = tbl.query(KeyConditionExpression=Key(key).eq(value) & Key(key2).eq(value2))
    df = pd.DataFrame(response['Items'])
    return df

table = 'table_name'
key = 'column_name1'
value = '2080000'
key2 = 'column_name2'
value2 = '2080000'

df = query_table(table, key, value, key2, value2)


#Scan table 1 condition
def scan_table(table, key, value):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    response = tbl.scan(FilterExpression=Attr(key).eq(value))
    df = pd.DataFrame(response['Items'])
    return df

table = 'table_name'
key = 'Status'
value = 'PROCESSING'

df = scan_table(table, key, value)


#scan multiple conditions (type equal)
def scan_multiple(table, filters):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    response = tbl.scan(FilterExpression=reduce(And, ([Key(k).eq(v) for k, v in filters.items()])))
    df = pd.DataFrame(response['Items'])
    return df

table = 'table_name'
filters = {'Status': 'ERROR', 'Date': '2021-05-01'}
#FilterExpression = ' AND '.join(['{0}=:{0}'.format(k) for k, v in filters.items()])
#ExpressionAttributeValues = {': {}'.format(k): {'S': v} for k, v in filters.items()}

df = scan_multiple(table, filters)


#Scan multiple conditions (different type conditions) [retorna vazio]
def scan_multiple2(table, filters, expression_values, expression_names):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    response = tbl.scan(FilterExpression=filters, ExpressionAttributeValues=expression_values, ExpressionAttributeNames=expression_names) #ExpressionAttributesValues / ExpressionAttributesNames
    df = pd.DataFrame(response['Items'])
    return df

table = 'table_name'
filters = '#Status=:st AND contains(Date, :dt)'
expression_values = {':st': {'S': 'ERROR'}, ':dt': {'S': '2021-12-10'}}
expression_names = {'#Status': 'Status'}

df = scan_multiple2(table, filters, expression_values, expression_names)


#Scan multiple conditions (different type conditions)
def scan_multiple3(table, key, key2, value, value2):
    dynamodb = boto3.resource('dynamodb')
    tbl = dynamodb.Table(table)
    response = tbl.scan(FilterExpression=Attr(key).eq(value) & Attr(key2).contains(value2))
    df = pd.DataFrame(response['Items'])
    return df

table = 'table_name'
key = 'Status'
key2 = 'PROCESSING'
value = 'OperationDate'
value2 = '2021-08-10'

df = scan_multiple3(table, key, key2, value, value2)


#multiple inserts
def insert_multiple(table, batch_list):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table)
    with table.batch_writer() as batch:
        for item in batch_list:
            batch.put_item(Item=item)
    return {'Status Code': 200, 'Message': 'Registros inseridos com sucesso'}

table = 'table_name-issue-notification'
batch_list = [{'Id': 'asd123', 'Payload': "{'user': 1, 'product': 'ted', 'amount': '100.00'}"}, {'Id': 'asd123', 'Payload': "{'user': 1, 'product': 'ted', 'amount': '100.00'}"}]

insert_multiple(table, batch_list)
