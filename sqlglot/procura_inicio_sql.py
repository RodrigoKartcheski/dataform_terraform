# Função para procurar a primeira cláusula SQL válida
def find_sql_clause_start(content, sql_clauses):
    for i, line in enumerate(content):
        # Verifica se a linha contém qualquer uma das palavras-chave SQL definidas
        if any(clause in line.upper() for clause in sql_clauses):
            return i + 1  # Retorna o número da linha (começando de 1)
    raise ValueError("Nenhuma palavra-chave SQL válida encontrada.")

# Ler o conteúdo do arquivo SQL
file_path = "validsql.sqlx"  # Caminho para o arquivo SQL
with open(file_path, 'r') as file:
    content = file.readlines()  # Lê todas as linhas do arquivo

# Definir as palavras-chave SQL para procurar
sql_clauses = ['WITH', 'SELECT', 'CREATE', 'INSERT', 'UPDATE', 'DELETE', 'ALTER']

# Encontrar o número da linha que contém a primeira cláusula SQL válida
sql_clause_line = find_sql_clause_start(content, sql_clauses)

# Exibir o número da linha onde a cláusula SQL foi encontrada
print(f"A cláusula SQL começa na linha: {sql_clause_line}")
