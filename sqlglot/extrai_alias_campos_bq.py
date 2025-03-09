import os
from google.cloud import bigquery
import sqlglot
import google.api_core.exceptions  # Importando o módulo correto para lidar com exceções


# Definir o caminho do arquivo de credenciais JSON
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/jobs/dataform_terraform/sqlglot/dataplex-experience-6133-b3882e4b6fdc.json"

def extract_column_aliases(sql_query):
    parsed = sqlglot.parse_one(sql_query)
    aliases = []

    for expression in parsed.expressions:
        if isinstance(expression, sqlglot.expressions.Alias):
            column_name = expression.this.sql()  # Nome original da coluna ou valor
            alias_name = expression.alias  # Nome do alias

            if isinstance(expression.this, sqlglot.expressions.Literal):
                expr_type = "literals"
                has_table_ref = False
            elif isinstance(expression.this, sqlglot.expressions.Column) and expression.this.table:
                expr_type = "value"
                has_table_ref = True
            else:
                expr_type = "value"
                has_table_ref = False

            aliases.append((column_name, alias_name, expr_type, has_table_ref))
        elif isinstance(expression, sqlglot.expressions.Column):
            column_name = expression.sql()
            has_table_ref = bool(expression.table)
            aliases.append((column_name, None, "value", has_table_ref))

    return aliases

def create_table_if_not_exists(client, dataset_id, table_id):
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    
    try:
        # Verifica se a tabela existe
        client.get_table(table_ref)
        print(f"Tabela {table_id} já existe.")
    except google.api_core.exceptions.NotFound:
        # Se não existir, cria a tabela com um esquema padrão
        schema = [
            bigquery.SchemaField("column_name", "STRING"),
            bigquery.SchemaField("alias_name", "STRING"),
            bigquery.SchemaField("expr_type", "STRING"),
            bigquery.SchemaField("has_table_ref", "BOOLEAN"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)  # Cria a tabela
        print(f"Tabela {table_id} criada com sucesso.")

def insert_into_bigquery(data, project_id, dataset_id, table_id):
    # Instancia o cliente do BigQuery
    client = bigquery.Client(project=project_id)

    # Cria a tabela caso não exista
    create_table_if_not_exists(client, dataset_id, table_id)
    
    # Prepara os dados para inserção
    rows_to_insert = []
    for row in data:
        rows_to_insert.append({
            'column_name': row[0],
            'alias_name': row[1],
            'expr_type': row[2],
            'has_table_ref': row[3]
        })
    
    # Referência à tabela
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # Verifica a tabela existente

    # Insere os dados na tabela
    errors = client.insert_rows_json(table, rows_to_insert)
    
    if errors:
        print(f"Erro ao inserir dados: {errors}")
    else:
        print(f"Dados inseridos com sucesso na tabela {table_id}.")

# Exemplo de uso
sql_query = """
SELECT
    p.x xx,
    p.zz as zza,
    tt,
    'SOMETHING' yy
FROM polls p;
"""

# Extrai as informações da query
aliases = extract_column_aliases(sql_query)

# Chama a função para inserir os dados no BigQuery
project_id = 'dataplex-experience-6133'
dataset_id = 'data_conjunto'
table_id = 'tabelaa'

insert_into_bigquery(aliases, project_id, dataset_id, table_id)
