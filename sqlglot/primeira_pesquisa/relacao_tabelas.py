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
*
from tab3
"""

dependencies = {}

for cte in parse_one(query).find_all(exp.CTE):
  dependencies[cte.alias_or_name] = []

  cte_query = cte.this.sql()
  for table in parse_one(cte_query).find_all(exp.Table):
    dependencies[cte.alias_or_name].append(table.name)
print(dependencies)