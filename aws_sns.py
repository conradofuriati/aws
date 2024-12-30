
import os
import json
import boto3
import pandas as pd

os.chdir('C:\\test')


#create topic
def create_topic(name):
    sns = boto3.resource('sns')
    topic = sns.create_topic(Name=name)
    return topic

create_topic('topic_name')


#list topic
def list_topics():
    sns = boto3.resource('sns')
    topics = sns.topic.all()
    return topics

for topic in list_topics():
    print(topic)


#subscribe
def subscribe(topic, protocol, endpoint):
    sns = boto3.client('sns')
    sub = sns.subscribe(TopicArn=topic, Protocol=protocol, Endpoint=endpoint, ReturnSubscriptionArn=True)
    return sub

topic = 'arn:aws:sns:sa-east-1:XXX:nome-topico'

email = 'conrado.dias@gmail.com'
email_sub = subscribe(topic, 'email', email)

sqs = 'arn:aws:sqs:sa-east-1:XXX:nome-fila'
sqs_sub = subscribe(topic, 'sqs', sqs)


#publish
def publish_message(topic, tittle, message):
    sns = boto3.client('sns')
    response = sns.publish(TopicArn=topic, Subject=title, Message=json.dumps(message))
    return response

topic = 'arn:aws:sa-east-1:XXX:'
title = 'Pagamento realizado com sucesso'
message = {'idPay': '1', 'Origin': 'BAAS', 'message': 'Pagamento realizado com sucesso'}
#message = ''

publish_message(topic, title, message)

