import sqlglot

def extract_column_aliases(sql_query):
    """
    Extrai informações sobre as colunas e aliases de uma query SQL.
    
    Retorna uma lista de tuplas no formato:
    [(coluna_original, alias, tipo, tem_referencia_tabela)]
    
    - coluna_original: Nome original da coluna ou valor literal.
    - alias: Nome do alias, se existir.
    - tipo: "value" para colunas e "literals" para valores fixos.
    - tem_referencia_tabela: True se a coluna tem referência a uma tabela, False caso contrário.
    """
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

# Caminho do arquivo SQL
file_path = "validsql.sqlx"  # Supondo que o arquivo esteja no mesmo diretório que o script

# Ler o conteúdo do arquivo SQL
with open(file_path, 'r') as file:
    sql_query = file.read()  # Lê todo o conteúdo do arquivo como uma string

# Chama a função de extração de aliases
aliases = extract_column_aliases(sql_query)

# Exibe os aliases extraídos
print(aliases)
