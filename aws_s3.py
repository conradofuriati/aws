
import os
import json
import boto3
import pandas as pd
from datetime import datetime

os.chdir('C:\\test')


#create bucket
def create_bucket(name):
    s3 = boto3.client('s3')
    response = s3.create_bucket(Bucket=name, CreateBucketConfiguration={'LocationConstraint': 'sa-east-1'})
    return response

bucket_name = 'bill-bankslip'
create_bucket(bucket_name)


#list buckets
def list_buckets():
    s3 = boto3.client('s3')
    response = s3.list_buckets()
    b = pd.DataFrame([i['Name'] for i in response['Buckets']])
    return b

list_buckets = list_buckets()


#list_objects
def list_objects(name):
    s3 = boto3.client('s3')
    response = s3.list_objects(Bucket=name)
    objects = [obj['Key'] for obj in response['Contents']]
    return objects

bucket_name = 'bill-bankslip'
list_objects(bucket_name)


#upload df
def upload_df(file_name, bucket, message):
    s3 = boto3.client('s3')
    response = s3.put_object(Key=file_name, Bucket=bucket, Body=message)
    return response

file_name = f'bankslip{datetime.now()}.txt'
bucket = 'bill-bankslip'
message = 'Boleto pago com sucesso'

upload_df(file_name, bucket, message)


#upload file
def upload_file(bucket, file_name):
    s3 = boto3.resource('s3')
    response = s3.meta.client.upload_file(Filename=file_name, Bucket=bucket, Key=file_name)
    return response

file_name = 'myfunction.zip'
bucket = 'bill-bankslip'

upload_file(bucket, file_name)


#download file
def download_file(bucket, file_name):
    s3 = boto3.client('s3')
    response = s3.download_file(bucket, file_name, file_name)
    return response

file_name = 'a123.txt'
bucket = 'bill-bankslip'

download_file(bucket, file_name)

