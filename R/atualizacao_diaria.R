#Atualização diária dos bancos de preços e retornos

#Carregar funções necessárias
source("R/Funcoes.R")


# Conectar ao banco de dados
con <- conectar_banco_dados()


# Carregar a lista de tickers
lista_tickers <- read.csv("data-raw/lista_ativos.csv", stringsAsFactors = FALSE)


#Atualização diária
for (ticker in lista_tickers$ticker) {
  registrar_log(sprintf("Processando ticker: %s", ticker))
  
  # Obter o ID do ticker na tabela Ativos
  resultado_query <- dbGetQuery(con, sprintf("SELECT id FROM Ativos WHERE ticker = '%s'", ticker))
  
  # Verificar se a consulta retornou algum resultado
  if (nrow(resultado_query) > 0) {
    ticker_id <- resultado_query[1, 1]
    
    atualizar_precos_diarios(con, ticker, ticker_id)
    
    atualizar_retornos_diarios(con, ticker_id)
    
    registrar_log(sprintf("Ticker %s processado com sucesso.", ticker))
    
  } else {
    registrar_log(sprintf("Ticker %s não encontrado na tabela Ativos.", ticker))
  }
}