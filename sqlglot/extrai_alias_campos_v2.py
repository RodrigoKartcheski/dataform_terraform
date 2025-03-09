'''import sqlglot
s = """
select
 p.x xx
 , 'SOMETHING' yy,
from
polls p
;
"""
aliases = [i.alias for i in sqlglot.parse_one(s).expressions]
print(aliases)'''

'''
import sqlglot

def extract_column_aliases(sql_query):
    parsed = sqlglot.parse_one(sql_query)
    aliases = []
    
    for expression in parsed.expressions:
        if isinstance(expression, sqlglot.expressions.Alias):
            column_name = expression.this.sql()  # Nome original da coluna
            alias_name = expression.alias  # Nome do alias
            aliases.append((column_name, alias_name))
    
    return aliases

# Exemplo de uso
sql_query = """
SELECT
    p.x xx,
    'SOMETHING' yy
FROM polls p;
"""

aliases = extract_column_aliases(sql_query)
print(aliases)
'''
'''
import sqlglot

def extract_column_aliases(sql_query):
    parsed = sqlglot.parse_one(sql_query)
    aliases = []
    
    for expression in parsed.expressions:
        if isinstance(expression, sqlglot.expressions.Alias) and not isinstance(expression.this, sqlglot.expressions.Literal):
            column_name = expression.this.sql()  # Nome original da coluna
            alias_name = expression.alias  # Nome do alias
            aliases.append((column_name, alias_name))
    
    return aliases

# Exemplo de uso
sql_query = """
SELECT
    p.x xx,
    'SOMETHING' yy
FROM polls p;
"""

aliases = extract_column_aliases(sql_query)
print(aliases)
'''

'''
import sqlglot

def extract_column_aliases(sql_query):
    parsed = sqlglot.parse_one(sql_query)
    aliases = []
    
    for expression in parsed.expressions:
        if isinstance(expression, sqlglot.expressions.Alias):
            column_name = expression.this.sql()  # Nome original da coluna ou valor
            alias_name = expression.alias  # Nome do alias
            
            if isinstance(expression.this, sqlglot.expressions.Literal):
                expr_type = "literals"
            else:
                expr_type = "value"
            
            aliases.append((column_name, alias_name, expr_type))
    
    return aliases

# Exemplo de uso
sql_query = """
SELECT
    p.x xx,
    zz as zza,
    tt,
    'SOMETHING' yy
FROM polls p;
"""

aliases = extract_column_aliases(sql_query)
print(aliases)
'''

'''
import sqlglot

def extract_column_aliases(sql_query):
    parsed = sqlglot.parse_one(sql_query)
    aliases = []
    
    for expression in parsed.expressions:
        if isinstance(expression, sqlglot.expressions.Alias):
            column_name = expression.this.sql()  # Nome original da coluna ou valor
            alias_name = expression.alias  # Nome do alias
            
            if isinstance(expression.this, sqlglot.expressions.Literal):
                expr_type = "literals"
            else:
                expr_type = "value"
            
            aliases.append((column_name, alias_name, expr_type))
        elif isinstance(expression, (sqlglot.expressions.Column, sqlglot.expressions.Identifier)):
            column_name = expression.sql()
            aliases.append((column_name, None, "value"))
    
    return aliases

# Exemplo de uso
sql_query = """
SELECT
    p.x xx,
    zz as zza,
    tt,
    1 ta,
    p.tb,
    'SOMETHING' yy
FROM polls p;
"""

aliases = extract_column_aliases(sql_query)
print(aliases)
'''

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
    
    Exemplo de saída:
    [('p.x', 'xx', 'value', True),   # 'p.x' tem alias 'xx' e pertence a uma tabela
     ('p.zz', 'zza', 'value', True), # 'p.zz' tem alias 'zza' e pertence a uma tabela
     ('tt', None, 'value', True),  # 't.tt' não tem alias e não tem referencia a uma tabela
     ("'SOMETHING'", 'yy', 'literals', False)] # Literal com alias 'yy' e sem referência a tabela
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

# Exemplo de uso
sql_query = """
SELECT
    p.x xx,
    p.zz as zza,
    tt,
    'SOMETHING' yy
FROM polls p;
"""
object_name = "table_new"

aliases = extract_column_aliases(sql_query)
print(aliases)
