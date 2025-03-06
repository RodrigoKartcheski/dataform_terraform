from sqlglot import parse_one, exp

query = """
with tab1 as
(
  select a,b from db1.table1
)
,tab2 as
(
  select a from tab1
)
,tab3 as
(
  select
  t1.a
  ,t2.b
  from tab1 t1
  join tab2 t2
  on t1.a = t2.a
)
select
a
from tab3
"""

dependencies = {}

# Parse the query
parsed_query = parse_one(query)

# Iterate through all CTEs (Common Table Expressions)
for cte in parsed_query.find_all(exp.CTE):
    dependencies[cte.alias_or_name] = []

    cte_query = cte.this.sql()

    # Parse CTE query to look for tables used
    for table in parse_one(cte_query).find_all(exp.Table):
        dependencies[cte.alias_or_name].append(table.name)

# Look for the SELECT * in the main query
select_star = False
for node in parsed_query.find_all(exp.Select):
    # Check if the SELECT has '*' in its columns (by looking for exp.Star)
    if isinstance(node.selects[0], exp.Star):
        select_star = True
        break

# Output dependencies and check for SELECT *
print("CTE Dependencies:", dependencies)
print("Contains SELECT *:", select_star)
