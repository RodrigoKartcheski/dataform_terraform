import sqlglot

def extract_column_aliases(sql_query):
    parsed = sqlglot.parse_one(sql_query)
    aliases = {}
    
    for select in parsed.find_all(sqlglot.expressions.Select):
        for expression in select.expressions:
            if isinstance(expression, sqlglot.expressions.Alias):
                column_name = expression.this.sql()  # Nome original da coluna
                alias_name = expression.alias  # Nome do alias
                aliases[column_name] = alias_name
    
    return aliases

# Exemplo de uso
sql_query = """
    SELECT 
        first_name AS fname, 
        last_name AS lname, 
        age, 
        salary * 1.1 AS adjusted_salary
    FROM employees
"""

aliases = extract_column_aliases(sql_query)
print(aliases)
