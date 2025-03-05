import sqlparse
from sqlparse.sql import Identifier, IdentifierList, Where, TokenList
from sqlparse.tokens import Keyword, DML
import os

# Função para identificar CTEs
def get_cte_names(parsed_query):
    cte_names = set()
    for stmt in parsed_query:
        # Procurando pela palavra-chave 'WITH' (indicando CTEs)
        if stmt.get_type() == 'UNKNOWN' and 'WITH' in str(stmt).upper():
            # Encontrar todas as expressões de CTE
            for token in stmt.tokens:
                if isinstance(token, Keyword) and token.value.upper() == "WITH":
                    # A CTE pode estar no próximo token
                    next_token = stmt.token_next(stmt.token_index(token))
                    if isinstance(next_token, TokenList):
                        # Adiciona os aliases de CTE
                        for sub_token in next_token.tokens:
                            if isinstance(sub_token, Identifier):
                                cte_names.add(sub_token.get_real_name())
    return cte_names

# Função para verificar se a seleção é de um CTE ou subconsulta
def is_from_cte_or_subquery(select_expression, cte_names):
    froms = select_expression.find_all(Identifier)
    tables = [x.get_real_name() for x in froms]
    subqueries = select_expression.find_all(DML)

    # Se há mais de uma tabela ou subconsulta, trata-se de uma subconsulta
    if len(tables) > 1 or len(subqueries) > 0:
        return True

    # Se alguma das tabelas está nos CTEs, é um CTE
    if len(set(tables) & set(cte_names)) > 0:
        return True

    return False

# Função principal para validação
def validate_sqlx(file_path):
    with open(file_path, 'r') as file:
        sql = file.read()
    
    parsed_query = sqlparse.parse(sql)
    cte_names = get_cte_names(parsed_query)
    
    for stmt in parsed_query:
        if isinstance(stmt, sqlparse.sql.Select):
            for selection in stmt.tokens:
                if isinstance(selection, sqlparse.sql.Identifier):
                    # Se a seleção for '*', vamos verificar
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
