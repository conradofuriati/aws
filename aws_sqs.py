
import os
import json
import boto3
import pandas as pd

os.chdir('C:\\test')


#create queue
def create_queue(name):
    sqs = boto3.client('sqs')
    response = sqs.create_queue(QueueName=name)
    return response

create_queue('bill-bankslip')


#create policy
def create_policy(queue, topic, policy):
    sqs = boto3.client('sqs')
    response = sqs.set_queue_attributes(QueueUrl=queue, Attributes={'Policy': policy})
    return response

queue = 'arn:aws:sa-east-1:XXX:nome-fila'
topic = 'arn:aws:sa-east-1:XXX:nome-topico'
policy = f'''{{buscar_json_internet}}'''

create_policy(queue, topic, policy)


#get queue
def get_queue(name):
    sqs = boto3.client('sqs')
    response = sqs.get_queue_url(QueueName=name)
    return response

df_queue = get_queue('nome_fila')


#list queues
def list_queues(prefix):
    sqs = boto3.client('sqs')
    response = sqs.list_queues(QueueNamePrefix=prefix)
    list_queues = pd.DataFrame([url for url in response['QueueUrls']])
    return list_queues

prefix = 'bill-'
list_queues = list_queues(prefix)


#send message - attributes não é obrigatorio
def send_message(queue, message, attributes):
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName=name)
    response = queue.send_message(MessageBody=message, MessageAttributes=attributes)
    return response

queue = 'nome_fila'
message = 'Pagamento realizado com sucesso'
attributes = {'bankslip': {'StringValue': '001', 'DataType': 'S'}, 'bankslip-settler': {'StringValue': '002', 'DataType': 'S'}}

send_message(queue, message, attributes)


#receive message
def receive_message(queue):
    sqs = boto3.client('sqs')
    response = sqs.receive_message(QueueUrl=queue, WaitTimeSeconds=5, VisibilityTimeout=5, MaxNumberOfMessages=10)
    return response

queue = 'https://sqs.sa-eat-1.amazonaws.com/XXX/nome-fila'

df_msg = receive_message(queue)

msg0 = df_msg['Messages']
msg1 = df_msg['Messages'][0]
msg2 = df_msg['Messages'][0]['Body']
msg3 = json.loads(df_msg['Messages'][0]['Body'])['Message']


#delete message
def delete_message(queue, handle):
    sqs = boto3.resource('sqs')
    response = sqs.delete_message(QueueUrl=queue, ReceiptHandle=handle)
    return response

queue = 'https://sqs.sa-eat-1.amazonaws.com/XXX/nome-fila'
handle = ''

delete_message(queue, handle)


#purge queue [limpar]
def purge_queue(queue):
    sqs = boto3.client('sqs')
    response = sqs.purge_queue(QueueUrl=queue)
    return response

queue = 'https://sqs.sa-eat-1.amazonaws.com/XXX/nome-fila'

purge_queue(queue)

