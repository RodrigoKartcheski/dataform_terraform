import sqlparse
from sqlparse.sql import Identifier, TokenList, Parenthesis
from sqlparse.tokens import Keyword, DML
import os

# Função para identificar CTEs
def get_cte_names(parsed_query):
    cte_names = set()
    for stmt in parsed_query:
        for token in stmt.tokens:
            if isinstance(token, sqlparse.sql.Token) and token.value.lower() == 'with':
                # Encontramos a palavra-chave "WITH", que é usada para CTEs
                for subtoken in stmt.tokens:
                    if isinstance(subtoken, sqlparse.sql.Identifier):
                        cte_names.add(subtoken.get_real_name())
    return cte_names

# Função para verificar se a seleção é de um CTE ou de uma subconsulta
def is_from_cte_or_subquery(select_expression, cte_names):
    froms = select_expression.find_all(Identifier)
    tables = [x.get_real_name() for x in froms]
    subqueries = select_expression.find_all(Parenthesis)
    
    # Se há mais de uma tabela ou subconsulta, trata-se de uma subconsulta
    if len(tables) > 1 or len(subqueries) > 0:
        return True
    
    # Se alguma das tabelas está nos CTEs, é um CTE
    if len(set(tables) & set(cte_names)) > 0:
        return True
    
    return False

# Função para validar a SQL
def validate_sqlx(file_path):
    with open(file_path, 'r') as file:
        sql = file.read()

    parsed_query = sqlparse.parse(sql)
    cte_names = get_cte_names(parsed_query)

    for stmt in parsed_query:
        if isinstance(stmt, TokenList):
            # Verifica se o comando é SELECT
            if stmt.get_type() == 'SELECT':
                for selection in stmt.tokens:
                    if isinstance(selection, Identifier):
                        # Se a seleção for '*' (SELECT *), vamos verificar
                        if selection.get_real_name() == "*":
                            if not is_from_cte_or_subquery(stmt, cte_names):
                                raise Exception(f"SELECT * only allowed from a CTE in {file_path}.")
    print(f"File {file_path} passed validation.")

# Rodar a validação para todos os arquivos .sqlx
def run_validation():
    for root, dirs, files in os.walk("."):
        for file in files:
            if file.endswith(".sqlx"):
                validate_sqlx(os.path.join(root, file))

if __name__ == "__main__":
    run_validation()
