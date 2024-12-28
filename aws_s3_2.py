import boto3

def download_file_from_s3(bucket_name, s3_key, local_path):
    s3 = boto3.client('s3')
    try:
        s3.download_file(bucket_name, s3_key, local_path)
        print(f"Arquivo baixado: {local_path}")
    except Exception as e:
        print(f"Erro ao baixar o arquivo: {e}")

# Substitua com as informações do seu S3 e caminhos desejados
bucket_name = 'seu-bucket'
s3_key = 'caminho/para/o/arquivo.txt'
local_path = 'caminho/local/arquivo.txt'

download_file_from_s3(bucket_name, s3_key, local_path)


##list files no bucket s3

import boto3

# Defina suas credenciais do AWS
aws_access_key_id = 'SUA_ACCESS_KEY'
aws_secret_access_key = 'SUA_SECRET_KEY'

# Nome do bucket S3 que você deseja listar
bucket_name = 'NOME_DO_BUCKET'

# Crie uma instância do cliente S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Liste os objetos no bucket
response = s3.list_objects_v2(Bucket=bucket_name)

# Verifique se a lista de objetos está presente na resposta
if 'Contents' in response:
    objects = response['Contents']
    for obj in objects:
        print(f'Nome do arquivo: {obj["Key"]}, Tamanho: {obj["Size"]} bytes')
else:
    print(f'O bucket {bucket_name} está vazio ou não existe.')


##listar arquivos em uma pasta dentro do bucket

import boto3

# Defina suas credenciais do AWS
aws_access_key_id = 'SUA_ACCESS_KEY'
aws_secret_access_key = 'SUA_SECRET_KEY'

# Nome do bucket S3 que você deseja listar
bucket_name = 'NOME_DO_BUCKET'

# Prefixo da pasta dentro do bucket
folder_prefix = 'pasta/'  # Substitua 'pasta/' pelo nome da sua pasta

# Crie uma instância do cliente S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Liste os objetos no bucket com o prefixo da pasta
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_prefix)

# Verifique se a lista de objetos está presente na resposta
if 'Contents' in response:
    objects = response['Contents']
    for obj in objects:
        print(f'Nome do arquivo: {obj["Key"]}, Tamanho: {obj["Size"]} bytes')
else:
    print(f'A pasta {folder_prefix} no bucket {bucket_name} está vazia ou não existe.')


##listar arquivo especifico

import boto3

# Defina suas credenciais do AWS
aws_access_key_id = 'SUA_ACCESS_KEY'
aws_secret_access_key = 'SUA_SECRET_KEY'

# Nome do bucket S3 que você deseja listar
bucket_name = 'NOME_DO_BUCKET'

# Nome do arquivo específico que você deseja listar
file_name = 'caminho/para/o/seu/arquivo/nome_do_arquivo.extensao'  # Substitua pelo caminho do seu arquivo

# Crie uma instância do cliente S3
s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

# Liste os objetos no bucket com o prefixo do arquivo específico
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=file_name)

# Verifique se a lista de objetos está presente na resposta
if 'Contents' in response:
    objects = response['Contents']
    for obj in objects:
        if obj["Key"] == file_name:
            print(f'Nome do arquivo: {obj["Key"]}, Tamanho: {obj["Size"]} bytes')
else:
    print(f'O arquivo {file_name} no bucket {bucket_name} não foi encontrado.')

