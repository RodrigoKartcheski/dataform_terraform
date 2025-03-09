from sqlglot import parse_one, exp

query = """
SELECT
    col1,
    col2,
    col3
FROM db1.table1
"""

for table in parse_one(query).find_all(exp.Table):
    print(f"Table => {table.name} | DB => {table.db}")