from sqlglot import parse_one, exp

# Consulta SQL que queremos analisar
sql = """
with tab1 as (
  select a, b from db1.table1
)
, tab2 as (
  select a from tab1
)
, tab3 as (
  select t1.a, t2.b
  from tab1 t1
  join tab2 t2 on t1.a = t2.a
)
select *
from tab3
"""

# Analisando a consulta SQL
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
