
import os
import json
import boto3
from datetime import datetime

os.chdir('C:\\test')

#list lambdas


#create function


#invoke function



###
#Lambda Code: Inserir no Console - projeto lambda
import os
import json
import boto3
from datetime import datetime

def lambda_handler(event, context):

    body = json.loads(event['Records'][0]['body'])
    message = json.loads(body['Message'])
    message2 = body['Message']
    idReg = str(uuid.uuid4())

    print(type(body))
    print(body)
    print(message)
    print(message2)
    print(idReg)

    #DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('bill-bankslip')
    response_db = table.put_item(Item={'Id': idReg, 'Payload': message})
    print(response_db)

    #S3
    d = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    file_name = f'bankslip-{d}-lambda.txt'
    bucket = 'bill-bankslip'
    message_s3 = message2

    s3 = boto3.client('s3')
    response_s3 = s3.put_object(Key=file_name, Bucket=bucket, Body=message_s3)
    print(response_s3)

    #SNS [usar outro topic, senao cria loop]
    topic = ''
    title = 'pagamento realizado com sucesso'

    sns = boto3.client('sns')
    response_sns = sns.publish(TopicArn=topic, Subject=title, Message=json.dumps(message2))
    print(response_sns)


