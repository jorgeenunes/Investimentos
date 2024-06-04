# Instalar e carregar o pacote RSQLite
library(RSQLite)

# Nome do arquivo do banco de dados
db_file <- "data-raw/investmentos.db"

# Conectar ao banco de dados SQLite
con <- dbConnect(SQLite(), dbname = db_file)

# Função para executar comandos SQL sem retorno de dados
exec_sql <- function(sql) {
  dbSendQuery(con, sql)
  invisible(NULL)
}

# Criar tabela Ativos
sql <- sprintf(
  "CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    ticker TEXT NOT NULL UNIQUE,
    nome TEXT,
    setor TEXT
  );",
  "Ativos"
)
exec_sql(sql)

# Criar tabela Precos_Historicos
sql <- sprintf(
  "CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    ticker_id INTEGER,
    data DATE,
    preco_fechamento_ajustado REAL,
    FOREIGN KEY (ticker_id) REFERENCES %s(id)
  );",
  "Precos_Historicos", "Ativos"
)
exec_sql(sql)

# Criar tabela Retornos_Historicos
sql <- sprintf(
  "CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    ticker_id INTEGER,
    data DATE,
    retorno_diario REAL,
    FOREIGN KEY (ticker_id) REFERENCES %s(id)
  );",
  "Retornos_Historicos", "Ativos"
)
exec_sql(sql)

# Criar tabela Composicao_Carteira
sql <- sprintf(
  "CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    data DATE,
    ticker_id INTEGER,
    peso REAL,
    FOREIGN KEY (ticker_id) REFERENCES %s(id)
  );",
  "Composicao_Carteira", "Ativos"
)
exec_sql(sql)

# Criar tabela Metrica_Performance
sql <- sprintf(
  "CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    data DATE,
    retorno_esperado REAL,
    volatilidade REAL,
    var REAL,
    cvar REAL,
    indice_sharpe REAL,
    indice_sortino REAL
  );",
  "Metrica_Performance"
)
exec_sql(sql)

# Criar tabela Transacoes
sql <- sprintf(
  "CREATE TABLE IF NOT EXISTS %s (
    id INTEGER PRIMARY KEY,
    data DATE,
    ticker_id INTEGER,
    quantidade INTEGER,
    preco REAL,
    tipo_transacao TEXT,
    FOREIGN KEY (ticker_id) REFERENCES %s(id)
  );",
  "Transacoes", "Ativos"
)
exec_sql(sql)

# Fechar a conexão com o banco de dados
dbDisconnect(con)