
import os
import json
import boto3
import pandas as pd
from datetime import datetime

#log group
group_name = '/aws/lambda/nome_lambda'
dt = '2022/07/29/' #prefixo data datetime.now().strftime('%Y/%m/%d')

client = boto3.client('logs')

response = client.filter_log_events(logGroupName=group_name)
response = client.filter_log_events(logGroupName=group_name, logStreamNamePrefix=dt) #erro logStreamNamePrefix

lista = response['events']

reg = response['events'][0]

df = pd.DataFrame(lista)


#log insights: query

#create log stream
#insert log

#describe_log_streams
#https://stackoverflow.com/questions/59240107/how-to-query-cloudwatch-logs-using-boto3-in-python

#https://stackoverflow.com/questions/62246997/how-to-filter-cloudwatch-logs-using-boto3-in-python


##aplicação para automatizar teste da aplicação principal
#teste automatizado
#request endpoint
#verifica json response
#verifica status tbl dynamo
#verifica log lambda notificação
#valida cenario - ok/erro
