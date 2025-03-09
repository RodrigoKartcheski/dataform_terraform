import os
from google.cloud import bigquery
import sqlglot
import google.api_core.exceptions  # Importando o módulo correto para lidar com exceções
from datetime import datetime

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

            aliases.append(("tb_hello_word", column_name, alias_name, expr_type, has_table_ref, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        elif isinstance(expression, sqlglot.expressions.Column):
            column_name = expression.sql()
            has_table_ref = bool(expression.table)
            aliases.append(("tb_hello_word", column_name, None, "value", has_table_ref, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

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
            bigquery.SchemaField("table_name", "STRING"),
            bigquery.SchemaField("column_name", "STRING"),
            bigquery.SchemaField("alias_name", "STRING"),
            bigquery.SchemaField("expr_type", "STRING"),
            bigquery.SchemaField("has_table_ref", "BOOLEAN"),
            bigquery.SchemaField("update", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)  # Cria a tabela
        print(f"Tabela {table_id} criada com sucesso.")

def is_duplicate(client, dataset_id, table_id, row):
    """ Verifica se os dados já existem na tabela, tratando valores NULL """
    query = f"""
        SELECT COUNT(*) AS count
        FROM `{dataset_id}.{table_id}`
        WHERE COALESCE(column_name, '') = COALESCE(@column_name, '')
        AND COALESCE(alias_name, '') = COALESCE(@alias_name, '')
    """
    
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("column_name", "STRING", row[1]),
            bigquery.ScalarQueryParameter("alias_name", "STRING", row[2]),
        ]
    )

    # Executa a consulta para verificar duplicatas
    query_job = client.query(query, job_config=job_config)
    result = query_job.result()
    
    # Se a contagem for maior que 0, é um duplicado
    for row in result:
        if row["count"] > 0:
            return True
    return False

def insert_into_bigquery(data, project_id, dataset_id, table_id):
    # Instancia o cliente do BigQuery
    client = bigquery.Client(project=project_id)

    # Cria a tabela caso não exista
    create_table_if_not_exists(client, dataset_id, table_id)
    
    # Prepara os dados para inserção
    rows_to_insert = []
    for row in data:
        # Verifica se a linha já existe na tabela antes de inseri-la
        if not is_duplicate(client, dataset_id, table_id, row):
            rows_to_insert.append({
                'table_name': row[0],
                'column_name': row[1],
                'alias_name': row[2],
                'expr_type': row[3],
                'has_table_ref': row[4],
                'update': row[5],
            })
        else:
            print(f"Registro duplicado encontrado e não inserido: {row}")
    
    # Referência à tabela
    table_ref = client.dataset(dataset_id).table(table_id)
    table = client.get_table(table_ref)  # Verifica a tabela existente

    # Insere os dados na tabela
    if rows_to_insert:
        errors = client.insert_rows_json(table, rows_to_insert)
        
        if errors:
            print(f"Erro ao inserir dados: {errors}")
        else:
            print(f"Dados inseridos com sucesso na tabela {table_id}.")
    else:
        print("Nenhum dado a ser inserido.")

# Exemplo de uso
sql_query = """
SELECT
    p.x xx,
    p.zz as zza,
    tt,
    'SOMETHING' yy,
    t.ano
FROM polls p;
"""

# Extrai as informações da query
aliases = extract_column_aliases(sql_query)

# Chama a função para inserir os dados no BigQuery
project_id = 'dataplex-experience-6133'
dataset_id = 'data_conjunto'
table_id = 'tabelaa'

insert_into_bigquery(aliases, project_id, dataset_id, table_id)
