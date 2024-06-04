# Carregar pacotes
library(RSQLite)
library(quantmod) 
library(dplyr)
library(lubridate)
library(R.utils)
library(bizdays)
library(yfR)

# Criar um calendário para o Brasil
cal <- create.calendar("Brazil/ANBIMA", weekdays=c("saturday", "sunday"))

# Nome do arquivo do banco de dados
db_file <- "data-raw/investmentos.db"

# Conectar ao banco de dados SQLite
con <- dbConnect(SQLite(), dbname = db_file)

# Função para baixar dados do Yahoo Finance
baixar_dados_yahoo <- function(ticker, data_inicio, data_fim) {
  tryCatchLog(
    {
      dados <- getSymbols(ticker, src = "yahoo", from = data_inicio, 
                          to = data_fim, auto.assign = FALSE)
      return(dados)
    }, 
    error = function(e) {
      message(sprintf("Erro ao baixar dados do ticker %s: %s", ticker, e$message))
      # Log do erro completo
      log.error(e, logger = "meu_app.log") 
      return(NULL) 
    }
  )
}

# Função para formatar os dados de preços
formatar_dados_precos <- function(dados, ticker_id) {
  if (!is.null(dados)) {
    dados <- data.frame(
      ticker_id = ticker_id,
      data = as.Date(index(dados)),  
      preco_fechamento_ajustado = as.numeric(Ad(dados))
    )
  }
  return(dados)
}

# Função para inserir dados em uma tabela
inserir_dados <- function(con, tabela, dados) {
  if (!is.null(dados) && nrow(dados) > 0) {
    dbAppendTable(con, tabela, dados)
  }
}

# Função para importar preços históricos
importar_precos_historicos <- function(con, ticker_id, data_inicio = "2000-01-01") {
  # Buscar a última data presente no banco de dados para o ticker
  query <- sprintf(
    "SELECT MAX(data) FROM Precos_Historicos WHERE ticker_id = %s",
    ticker_id
  )
  ultima_data <- dbGetQuery(con, query)
  
  if (nrow(ultima_data) > 0 && !is.na(ultima_data[1, 1])) {
    data_inicio <- as.Date(ultima_data[1, 1])
    data_fim <- Sys.Date()  
    data_inicio <- add.bizdays(data_inicio, 1, cal) # Próximo dia útil
    
    # Baixar dados apenas se data_fim for dia útil
    if (is.bizday(data_fim, cal)) {
      dados <- baixar_dados_yahoo(ticker, data_inicio, data_fim)
      dados <- formatar_dados_precos(dados, ticker_id)
      inserir_dados(con, "Precos_Historicos", dados)
    }
  } else {
    # Se não houver dados, usar data_inicio padrão (2000-01-01)
    dados <- baixar_dados_yahoo(ticker, data_inicio, Sys.Date())
    dados <- formatar_dados_precos(dados, ticker_id)
    inserir_dados(con, "Precos_Historicos", dados)
  }
}

atualizar_tabela_ativos <- function(con, ticker) {
  # Verificar se o ticker já existe na tabela
  query <- sprintf("SELECT 1 FROM Ativos WHERE ticker = '%s'", ticker)
  resultado <- dbGetQuery(con, query)
  
  if (nrow(resultado) == 0) {
    tryCatch({
      # Buscar informações do ativo usando yfR
      info_ativo <- yf_get_fundamentals(ticker, first_release_date = Sys.Date() - 365)
      
      # Extrair nome e setor
      nome <- info_ativo$longName[1]
      setor <- info_ativo$sector[1]
      
      # Tratar valores NA 
      if (is.na(setor)) {
        setor <- "Desconhecido" 
      }
      
      # Inserir na tabela Ativos
      query_insert <- sprintf(
        "INSERT INTO Ativos (ticker, nome, setor) VALUES ('%s', '%s', '%s')",
        ticker, nome, setor
      )
      dbExecute(con, query_insert)
      message(sprintf("Ticker %s adicionado à tabela Ativos.", ticker))
      
    }, error = function(e) {
      message(sprintf("Erro ao adicionar ticker %s à tabela Ativos: %s", ticker, e$message))
      print(e) 
    })
  }
}

#Carregar a lista de tickers
lista_tickers <- read.csv("data-raw/lista_ativos.csv", stringsAsFactors = FALSE)

#Iterar sobre a lista e importar os dados
for (ticker in lista_tickers$ticker) {
  #Atualizar tabela Ativos (se necessário)
  atualizar_tabela_ativos(con, ticker)
  
  # Obter o ID do ticker na tabela Ativos
  ticker_id <- dbGetQuery(con, sprintf("SELECT id FROM Ativos WHERE ticker = '%s'", ticker))[1, 1]
  
  #Importar preços históricos
  importar_precos_historicos(con, ticker_id)
}

# Fechar a conexão com o banco de dados
dbDisconnect(con)





