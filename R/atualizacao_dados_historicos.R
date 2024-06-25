# Carregar pacotes
library(RSQLite)
library(quantmod)
library(dplyr)
library(lubridate)
library(R.utils)
library(bizdays)
library(zoo)

# Configurar Log
logFile <- "logs/log_importacao.txt"

registrar_log <- function(mensagem) {
  linha <- paste(Sys.time(), mensagem, "\n", sep = " - ")
  cat(linha, file = logFile, append = TRUE)
  cat(linha)
}

# Criar um calendário para o Brasil
cal <- create.calendar("Brazil/ANBIMA", weekdays=c("saturday", "sunday"))

# Nome do arquivo do banco de dados
db_file <- "data/investmentos.db"

# Conectar ao banco de dados SQLite
con <- dbConnect(SQLite(), dbname = db_file)

# Função para baixar dados do Yahoo Finance com tratamento de erros
baixar_dados_yahoo <- function(ticker, data_inicio, data_fim) {
  tryCatch({
    dados <- getSymbols(ticker, src = "yahoo", from = data_inicio, 
                        to = data_fim, auto.assign = FALSE)
    if (is.null(dados) || nrow(dados) == 0) {
      stop(sprintf("Nenhum dado retornado para o ticker %s", ticker))
    }
    message(sprintf("Dados baixados para o ticker %s", ticker))
    return(dados)
  }, error = function(e) {
    mensagem <- sprintf("Erro ao baixar dados do ticker %s: %s", ticker, e$message)
    registrar_log(mensagem)
    warning(mensagem)
    return(NULL)
  })
}

# Função para formatar os dados de preços
formatar_dados_precos <- function(dados, ticker_id) {
  if (!is.null(dados)) {
    dados <- data.frame(
      ticker_id = ticker_id,
      # Converter a data para texto no formato "YYYY-MM-DD"
      data = format(as.Date(index(dados), origin = "1970-01-01"), "%Y-%m-%d"), 
      preco_fechamento_ajustado = as.numeric(Ad(dados))
    )
    
    # Preencher valores faltantes com o último valor válido
    dados <- dados %>% 
      mutate(preco_fechamento_ajustado = na.locf(preco_fechamento_ajustado))
    
    message(sprintf("Dados formatados para o ticker_id %d", ticker_id))
  }
  return(dados)
}

# Função para inserir dados em uma tabela
inserir_dados <- function(con, tabela, dados) {
  if (!is.null(dados) && nrow(dados) > 0) {
    # Verificar duplicação de dados
    dados_existentes <- dbGetQuery(con, sprintf(
      "SELECT data FROM %s WHERE ticker_id = %d AND data IN (%s)",
      tabela, dados$ticker_id[1], paste0(sprintf("'%s'", dados$data), collapse = ", ")
    ))
    
    if (nrow(dados_existentes) > 0) {
      dados <- dados[!dados$data %in% dados_existentes$data, ]
    }
    
    if (nrow(dados) > 0) {
      # Usar dbWriteTable com append = TRUE
      dbWriteTable(con, tabela, dados, append = TRUE)
      message(sprintf("Dados inseridos na tabela %s para o ticker_id %d", tabela, dados$ticker_id[1]))
    } else {
      message(sprintf("Nenhum dado novo para inserir na tabela %s para o ticker_id %d", tabela, dados$ticker_id[1]))
    }
  } else {
    message(sprintf("Nenhum dado para inserir na tabela %s para o ticker_id %d", tabela, ifelse(is.null(dados), NA, dados$ticker_id[1])))
  }
}

# Funções modulares para importar preços históricos
obter_ultima_data_banco <- function(con, ticker_id) {
  query <- sprintf("SELECT MAX(data) FROM Precos_Historicos WHERE ticker_id = %s", ticker_id)
  ultima_data <- dbGetQuery(con, query)
  
  if (nrow(ultima_data) > 0 && !is.na(ultima_data[1, 1])) {
    return(as.Date(ultima_data[1, 1]))
  } else {
    return(NULL)
  }
}

definir_periodo_download <- function(ultima_data, data_inicio) {
  if (!is.null(ultima_data)) {
    data_inicio <- add.bizdays(ultima_data, 1, cal) 
  } else {
    data_inicio <- as.Date(data_inicio)
  }
  data_fim <- Sys.Date()
  # Encontrar a última data útil
  while (!is.bizday(data_fim, cal)) {
    data_fim <- data_fim - 1
  }
  return(list(data_inicio = data_inicio, data_fim = data_fim))
}

importar_precos_historicos <- function(con, ticker, ticker_id, data_inicio = "2000-01-01") {
  ultima_data <- obter_ultima_data_banco(con, ticker_id)
  periodo <- definir_periodo_download(ultima_data, data_inicio)
  
  dados <- baixar_dados_yahoo(ticker, periodo$data_inicio, periodo$data_fim)
  
  if (!is.null(dados)) {
    dados <- formatar_dados_precos(dados, ticker_id)
    inserir_dados(con, "Precos_Historicos", dados)
  } else {
    message(sprintf("Dados nulos retornados para o ticker %s", ticker))
  }
}

# Função para atualizar a tabela Ativos com tratamento de erros
atualizar_tabela_ativos <- function(con, ticker) {
  # Verificar se o ticker já existe na tabela
  query <- sprintf("SELECT id FROM Ativos WHERE ticker = '%s'", ticker)
  resultado <- dbGetQuery(con, query)
  
  if (nrow(resultado) == 0) {
    tryCatch({
      # Buscar informações do ativo usando getQuote
      info_ativo <- getQuote(ticker)
      
      # Extrair nome (ou usar ticker como nome se não disponível)
      nome <- ifelse(!is.null(info_ativo$Name), info_ativo$Name, ticker)
      setor <- "Desconhecido" # Setor não disponível pelo getQuote
      
      # Inserir na tabela Ativos
      query_insert <- sprintf(
        "INSERT INTO Ativos (ticker, nome, setor) VALUES ('%s', '%s', '%s')",
        ticker, nome, setor
      )
      dbExecute(con, query_insert)
      message(sprintf("Ticker %s adicionado à tabela Ativos.", ticker))
      
    }, error = function(e) {
      mensagem <- sprintf("Erro ao adicionar ticker %s à tabela Ativos: %s", ticker, e$message)
      registrar_log(mensagem)
      warning(mensagem)
    })
  }
}

# Função para calcular e inserir retornos diários
calcular_e_inserir_retornos <- function(con, ticker_id) {
  # Buscar os preços históricos do ativo
  query <- sprintf("SELECT * FROM Precos_Historicos WHERE ticker_id = %d ORDER BY data", ticker_id)
  precos <- dbGetQuery(con, query)
  
  # Calcular o retorno diário
  precos <- precos %>%
    mutate(retorno_diario = (preco_fechamento_ajustado - lag(preco_fechamento_ajustado)) / 
             lag(preco_fechamento_ajustado))
  
  # Remover a primeira linha (que terá NA no retorno)
  precos <- precos[!is.na(precos$retorno_diario), ]
  
  # Inserir os retornos na tabela Retornos_Historicos
  if (nrow(precos) > 0) {
    inserir_dados(con, "Retornos_Historicos", precos %>% select(ticker_id, data, retorno_diario))
    message(sprintf("Retornos diários inseridos para o ticker_id %d", ticker_id))
  } else {
    message(sprintf("Nenhum retorno diário para inserir para o ticker_id %d", ticker_id))
  }
}

# Carregar a lista de tickers e validar
lista_tickers <- read.csv("data-raw/lista_ativos.csv", stringsAsFactors = FALSE)

# Verificar se há NA na coluna ticker
if (any(is.na(lista_tickers$ticker))) {
  mensagem <- "Existem valores NA na coluna 'ticker' do arquivo lista_ativos.csv. Corrija antes de prosseguir."
  registrar_log(mensagem)
  stop(mensagem)
}

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