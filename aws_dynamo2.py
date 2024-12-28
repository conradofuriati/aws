import boto3
from datetime import datetime
import pandas as pd

def query_dynamodb_to_dataframe(table_name):
    dynamodb = boto3.client('dynamodb')
    
    current_date = datetime.now().strftime('%Y-%m-%d')
    filter_expression = "MovementDate = :date"
    expression_values = {
        ":date": {"S": current_date}
    }
    
    try:
        response = dynamodb.scan(
            TableName=table_name,
            FilterExpression=filter_expression,
            ExpressionAttributeValues=expression_values
        )
        
        items = response.get('Items', [])
        
        # Criar uma lista de dicionários com os itens retornados
        data = []
        for item in items:
            data.append({key: list(value.values())[0] for key, value in item.items()})
        
        # Converter a lista de dicionários em um DataFrame
        df = pd.DataFrame(data)
        
        return df
    except Exception as e:
        print(f"Erro ao consultar a tabela: {e}")
        return None

# Substitua com o nome da sua tabela do DynamoDB
table_name = 'nome-da-sua-tabela'

result_df = query_dynamodb_to_dataframe(table_name)
if result_df is not None:
    print(result_df)


##
import boto3
import pandas as pd
from datetime import datetime

def query_dynamodb_to_dataframe(table_name, search_term):
    dynamodb = boto3.client('dynamodb')
    
    # Suponhamos que você queira filtrar registros onde a coluna 'Description' contém 'search_term'
    filter_expression = "contains(Description, :term)"
    expression_values = {
        ":term": {"S": search_term}
    }
    
    try:
        response = dynamodb.scan(
            TableName=table_name,
            FilterExpression=filter_expression,
            ExpressionAttributeValues=expression_values
        )
        
        items = response.get('Items', [])
        
        # Criar uma lista de dicionários com os itens retornados
        data = []
        for item in items:
            data.append({key: list(value.values())[0] for key, value in item.items()})
        
        # Converter a lista de dicionários em um DataFrame
        df = pd.DataFrame(data)
        
        return df
    except Exception as e:
        print(f"Erro ao consultar a tabela: {e}")
        return None

# Exemplo de uso:
table_name = "NomeDaSuaTabela"
search_term = "termo_de_pesquisa"
df = query_dynamodb_to_dataframe(table_name, search_term)
print(df)
