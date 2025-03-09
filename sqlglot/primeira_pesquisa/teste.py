import re
from sqlglot import parse_one, exp

# Função para procurar a primeira cláusula SQL válida
def find_sql_clause_start(content, sql_clauses):
    for i, line in enumerate(content):
        if any(clause in line.upper() for clause in sql_clauses):
            return i  # Retorna o índice da linha onde a cláusula SQL foi encontrada
    raise ValueError("Nenhuma palavra-chave SQL válida encontrada.")

# Função para validar a sintaxe do bloco `config`
def validate_config_block(content):
    config_start = False
    for line in content:
        if 'config {' in line:
            config_start = True
        if config_start:
            if '}' in line:
                return True  # Finaliza o bloco config
    return False  # Se o bloco não for finalizado corretamente

# Ler o conteúdo do arquivo SQL
file_path = "definitions/validsql.sqlx"  # Supondo que o arquivo está no mesmo diretório que o script
with open(file_path, 'r') as file:
    content = file.readlines() # Lê todas as linhas do arquivo

# Definir as palavras-chave SQL para procurar (com base em seu caso)
sql_clauses = ['WITH', 'SELECT', 'CREATE', 'INSERT', 'UPDATE', 'DELETE', 'ALTER']

# Encontrar o índice da primeira linha que contém uma cláusula SQL válida
sql_clause_index = find_sql_clause_start(content, sql_clauses)

# Juntar as linhas a partir da cláusula SQL encontrada
sql = "".join(content[sql_clause_index:])

# Verificar se o bloco config está presente e validado corretamente
if not validate_config_block(content):
    raise Exception("Erro: Bloco `config` não foi encontrado ou está mal formatado.")

# Analisando a consulta SQL com sqlglot
parsed_query = parse_one(sql, dialect="bigquery")

# Identificar todos os nomes de CTEs (Common Table Expressions)
cte_names = {cte.alias for cte in parsed_query.find_all(exp.CTE)}

# Função para verificar se a seleção vem de uma CTE ou subconsulta
def is_from_cte_or_subquery(select_expression):
    # Encontrar todas as tabelas da consulta
    froms = select_expression.find_all(exp.Table)
    tables = [x.args.get("this").args.get("this") for x in froms]
    
    # Encontrar subconsultas
    subqueries = select_expression.find_all(exp.Subquery)
    num_subqueries = len([_ for _ in subqueries])
    
    # Se a consulta usa mais de uma tabela ou contém subconsultas, retornar True
    if len(tables) > 1 or num_subqueries > 0:
        return True
    
    # Verificar se a consulta usa tabelas que estão nas CTEs
    if len(set(tables) & set(cte_names)) > 0:
        return True
    
    return False

# Iterar pelas expressões SELECT e verificar se contém SELECT *
for select in parsed_query.find_all(exp.Select):
    for selection in select.args.get("expressions", []):
        if isinstance(selection, exp.Star) and not is_from_cte_or_subquery(select):
            raise Exception(
                "select * is only allowed when selecting from a CTE.",
                select,
            )

# Caso o código chegue até aqui, significa que a verificação passou sem exceções
print("Consulta válida: 'SELECT *' está sendo usado corretamente.")
