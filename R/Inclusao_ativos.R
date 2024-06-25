# Carregar pacotes necessários
source("R/Funcoes.R")

# Conectar ao banco de dados
con <- conectar_banco_dados()

# Carregar a lista de tickers
lista_tickers <- read.csv("data-raw/lista_novos_ativos.csv", stringsAsFactors = FALSE)

# Iterar sobre a lista e importar os dados
for (ticker in lista_tickers$ticker) {
  registrar_log(sprintf("Processando ticker: %s", ticker))
  
  # Atualizar tabela Ativos (se necessário)
  atualizar_tabela_ativos(con, ticker)
  
  # Obter o ID do ticker na tabela Ativos
  ticker_id <- dbGetQuery(con, sprintf("SELECT id FROM Ativos WHERE ticker = '%s'", ticker))[1, 1]
  
  # Importar preços históricos
  importar_precos_historicos(con, ticker, ticker_id)
  
  # Calcular e inserir retornos diários
  calcular_e_inserir_retornos(con, ticker_id)
  
  registrar_log(sprintf("Ticker %s processado com sucesso.", ticker))
}

# Fechar a conexão com o banco de dados
dbDisconnect(con)






















