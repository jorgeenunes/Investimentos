#' Funções para importar, processar e inserir dados de ações no banco de dados.
#'
#' Este script contém funções para importar dados históricos de ações do Yahoo Finance,
#' processá-los e inseri-los em um banco de dados SQLite. Ele inclui funções para:
#' 
#' * Baixar dados históricos de preços de ações.
#' * Formatar dados de preços para inserção no banco de dados.
#' * Inserir dados em uma tabela, evitando duplicatas.
#' * Obter a última data disponível para um ativo no banco de dados.
#' * Definir o período de download de dados.
#' * Importar preços históricos para um ativo.
#' * Atualizar a tabela de ativos com novas informações.
#' * Calcular e inserir retornos diários.
#'
#' @docType package
#' @name Investimentos


#' @title Conectar ao banco de dados SQLite
#' @description Estabelece uma conexão com o banco de dados SQLite.
#' @param db_file Caminho para o arquivo do banco de dados SQLite.
#' @return Um objeto de conexão ao banco de dados.
#' @export
conectar_banco_dados <- function(db_file = "data/investmentos.db") {
  con <- dbConnect(SQLite(), dbname = db_file)
  return(con)
}

#' @title Registrar Log
#' @description Grava uma mensagem de log no console e em um arquivo de log.
#' @param mensagem Mensagem a ser registrada.
#' @param logFile Caminho para o arquivo de log.
registrar_log <- function(mensagem, logFile = "logs/log_importacao.txt") {
  linha <- paste(Sys.time(), mensagem, "\n", sep = " - ")
  cat(linha, file = logFile, append = TRUE)
  cat(linha)
}

#' @title Criar Calendário ANBIMA
#' @description Cria um objeto de calendário para o Brasil, considerando os feriados da ANBIMA.
#' @return Um objeto de calendário.
criar_calendario_anbima <- function() {
  create.calendar("Brazil/ANBIMA", weekdays = c("saturday", "sunday"))
}

#' @title Baixar Dados do Yahoo Finance
#' @description Baixa dados históricos de preços de ações do Yahoo Finance.
#' @param ticker O símbolo de ticker da ação.
#' @param data_inicio A data de início para os dados históricos.
#' @param data_fim A data de término para os dados históricos.
#' @return Um objeto xts contendo os dados históricos de preços, 
#'         ou NULL se ocorrer um erro no download.
baixar_dados_yahoo <- function(ticker, data_inicio, data_fim) {
  tryCatch({
    dados <- getSymbols(ticker, 
                        src = "yahoo", 
                        from = data_inicio, 
                        to = data_fim, 
                        auto.assign = FALSE)
    
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

#' @title Formatar Dados de Preços
#' @description Formata os dados de preços de ações para inserção no banco de dados.
#' @param dados Um objeto xts contendo os dados históricos de preços.
#' @param ticker_id O ID do ticker da ação no banco de dados.
#' @return Um data frame com os dados formatados, 
#'         ou NULL se a entrada 'dados' for NULL.
formatar_dados_precos <- function(dados, ticker_id) {
  if (!is.null(dados)) {
    dados <- data.frame(
      ticker_id = ticker_id,
      # Converter o índice para Date e depois formatar
      data = format(as.Date(index(dados)), "%Y-%m-%d"),
      preco_fechamento_ajustado = as.numeric(Ad(dados))
    )
    
    dados <- dados %>% 
      mutate(preco_fechamento_ajustado = zoo::na.locf(preco_fechamento_ajustado))
    
    message(sprintf("Dados formatados para o ticker_id %d", ticker_id))
    return(dados)
  } else {
    return(NULL)
  }
}

#' @title Inserir Dados
#' @description Insere dados em uma tabela do banco de dados, evitando duplicatas.
#' @param con Objeto de conexão com o banco de dados.
#' @param tabela Nome da tabela onde inserir os dados.
#' @param dados Data frame contendo os dados a serem inseridos.
# inserir_dados <- function(con, tabela, dados) {
#   if (!is.null(dados) && nrow(dados) > 0) {
#     for (i in 1:nrow(dados)) {
#       ticker_id <- dados$ticker_id[i]
#       data_str <- dados$data[i]
#       
#       # Consulta SQL com WHERE NOT EXISTS para evitar duplicatas
#       query <- sprintf(
#         "INSERT INTO %s (ticker_id, data, preco_fechamento_ajustado) 
#          SELECT %d, '%s', %f 
#          WHERE NOT EXISTS (
#            SELECT 1 
#            FROM %s 
#            WHERE ticker_id = %d AND data = '%s'
#          )", 
#         tabela, ticker_id, data_str, dados$preco_fechamento_ajustado[i],
#         tabela, ticker_id, data_str
#       )
#       
#       dbExecute(con, query)
#     }
#     
#     message(sprintf("%d linhas inseridas (ignorando duplicatas) na tabela %s.",
#                     nrow(dados), tabela))
#   } else {
#     message(sprintf("Nenhum dado para inserir na tabela %s.", tabela))
#   }
# }

#' @title Obter Última Data do Banco
#' @description Obtém a última data disponível para um ativo no banco de dados.
#' @param con Objeto de conexão com o banco de dados.
#' @param ticker_id O ID do ticker da ação no banco de dados.
#' @return A última data disponível como um objeto Date, 
#'         ou NULL se nenhuma data for encontrada.
obter_ultima_data_banco <- function(con, ticker_id) {
  query <- sprintf("SELECT MAX(data) FROM Precos_Historicos WHERE ticker_id = %s", ticker_id)
  ultima_data <- dbGetQuery(con, query)
  
  # Verificar se o valor é NA antes da conversão
  if (nrow(ultima_data) > 0 && !is.na(ultima_data[[1]])) {
    return(as.Date(ultima_data[[1]]))
  } else {
    return(NULL)
  }
}

#' @title Definir Período de Download
#' @description Define o período de download de dados com base na última data disponível.
#' @param ultima_data A última data disponível como um objeto Date.
#' @param data_inicio A data de início padrão, caso não haja dados anteriores.
#' @param cal Um objeto de calendário para calcular dias úteis.
#' @return Uma lista contendo a data de início e a data de fim para o download.
definir_periodo_download <- function(ultima_data, data_inicio, cal = criar_calendario_anbima()) {
  if (!is.null(ultima_data)) {
    data_inicio <- add.bizdays(ultima_data, 1, cal) 
  } else {
    data_inicio <- as.Date(data_inicio)
  }
  data_fim <- Sys.Date()
  while (!is.bizday(data_fim, cal)) {
    data_fim <- data_fim - 1
  }
  return(list(data_inicio = data_inicio, data_fim = data_fim))
}

#' @title Importar Preços Históricos
#' @description Importa preços históricos de ações para um ativo específico.
#' @param con Objeto de conexão com o banco de dados.
#' @param ticker O símbolo de ticker da ação.
#' @param ticker_id O ID do ticker da ação no banco de dados.
#' @param data_inicio A data de início padrão para dados históricos.
importar_precos_historicos <- function(con, ticker, ticker_id, data_inicio = "2000-01-01") {
  ultima_data <- obter_ultima_data_banco(con, ticker_id)
  periodo <- definir_periodo_download(ultima_data, data_inicio)
  
  dados <- baixar_dados_yahoo(ticker, periodo$data_inicio, periodo$data_fim)
  
  if (!is.null(dados)) {
    dados <- formatar_dados_precos(dados, ticker_id)
    inserir_precos_historicos(con, dados)
  } else {
    message(sprintf("Dados nulos retornados para o ticker %s", ticker))
  }
}

#' @title Atualizar Tabela de Ativos
#' @description Atualiza a tabela de ativos com novas informações.
#' @param con Objeto de conexão com o banco de dados.
#' @param ticker O símbolo de ticker da ação.
atualizar_tabela_ativos <- function(con, ticker) {
  query <- sprintf("SELECT id FROM Ativos WHERE ticker = '%s'", ticker)
  resultado <- dbGetQuery(con, query)
  
  if (nrow(resultado) == 0) {
    tryCatch({
      info_ativo <- getQuote(ticker)
      nome <- ifelse(!is.null(info_ativo$Name), info_ativo$Name, ticker)
      setor <- "Desconhecido" 
      
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

#' @title Calcular e Inserir Retornos Diários
#' @description Calcula e insere retornos diários para um ativo específico.
#' @param con Objeto de conexão com o banco de dados.
#' @param ticker_id O ID do ticker da ação no banco de dados.
calcular_e_inserir_retornos <- function(con, ticker_id) {
  query <- sprintf("SELECT * FROM Precos_Historicos WHERE ticker_id = %d ORDER BY data", ticker_id)
  precos <- dbGetQuery(con, query)
  
  precos <- precos %>%
    mutate(preco_fechamento_ajustado = zoo::na.locf(preco_fechamento_ajustado)) %>% 
    mutate(retorno_diario = (preco_fechamento_ajustado - lag(preco_fechamento_ajustado)) / 
             lag(preco_fechamento_ajustado),
           ticker = ticker) %>%
    na.omit()
  
  if (nrow(precos) > 0) {
    # Renomear a coluna antes de chamar inserir_dados
    inserir_retornos_diarios(con, precos %>% select( ticker_id, data, retorno_diario))
    message(sprintf("Retornos diários inseridos para o ticker_id %d", ticker_id))
  } else {
    message(sprintf("Nenhum retorno diário para inserir para o ticker_id %d", ticker_id))
  }
}

#' @title Atualizar Preços Diários
#' @description Atualiza os preços diários de ações no banco de dados.
#' @param con Objeto de conexão com o banco de dados.
#' @param ticker O símbolo de ticker da ação.
#' @param ticker_id O ID do ticker da ação no banco de dados.
atualizar_precos_diarios <- function(con, ticker, ticker_id) {
  ultima_data <- obter_ultima_data_banco(con, ticker_id)
  
  # Garante que 'ultima_data' seja sempre tratado como Date
  if (!is.null(ultima_data)) {
    ultima_data <- as.Date(ultima_data) 
  }
  
  # Se a última data for o dia útil anterior, não faz nada
  if (!is.null(ultima_data)) {
    ultima_data_formatada <- format(ultima_data, "%Y-%m-%d")
    dia_util_anterior <- bizdays::offset(Sys.Date(), -1, cal = criar_calendario_anbima())
    dia_util_anterior_formatado <- format(dia_util_anterior, "%Y-%m-%d")
    
    if (ultima_data_formatada == dia_util_anterior_formatado) {
      message(sprintf("Preços do ticker %s já estão atualizados.", ticker))
      return(invisible(NULL))
    }
  }
  
  periodo <- definir_periodo_download(ultima_data, ultima_data) 
  dados <- baixar_dados_yahoo(ticker, periodo$data_inicio, periodo$data_fim)
  
  if (!is.null(dados)) {
    dados <- formatar_dados_precos(dados, ticker_id)
    inserir_dados(con, "Precos_Historicos", dados)
    message(sprintf("Preços diários do ticker %s atualizados com sucesso.", ticker))
  } else {
    message(sprintf("Falha ao atualizar preços diários do ticker %s.", ticker))
  }
}

#' @title Atualizar Retornos Diários
#' @description Calcula e atualiza os retornos diários de um ativo no banco de dados.
#' @param con Objeto de conexão com o banco de dados.
#' @param ticker_id O ID do ticker da ação no banco de dados.
atualizar_retornos_diarios <- function(con, ticker_id) {
  # Obter a última data com retorno calculado
  query <- sprintf("SELECT MAX(data) FROM Retornos_Historicos WHERE ticker_id = %s", ticker_id)
  ultima_data_retorno <- dbGetQuery(con, query)
  ultima_data_retorno <- as.Date(ultima_data_retorno[[1]])
  
  # Obter a última data com preço disponível
  ultima_data_preco <- obter_ultima_data_banco(con, ticker_id)
  
  # Se a última data de preço for mais recente que a última data de retorno, calcular e inserir novos retornos
  if (ultima_data_preco > ultima_data_retorno) {
    calcular_e_inserir_retornos(con, ticker_id)
    message(sprintf("Retornos diários do ticker_id %d atualizados com sucesso.", ticker_id))
  } else {
    message(sprintf("Retornos diários do ticker_id %d já estão atualizados.", ticker_id))
  }
}


#' @title Inserir Retornos Diários
#' @description Insere retornos diários na tabela Retornos_Historicos, evitando duplicatas.
#' @param con Objeto de conexão com o banco de dados.
#' @param dados Data frame contendo os dados a serem inseridos.
inserir_retornos_diarios <- function(con, dados) {
  if (!is.null(dados) && nrow(dados) > 0) {
    for (i in 1:nrow(dados)) {
      ticker_id <- dados$ticker_id[i]
      data_str <- dados$data[i]
      retorno <- dados$retorno_diario[i]
      
      query <- sprintf(
        "INSERT INTO Retornos_Historicos (ticker_id, data, retorno_diario) 
         SELECT %d, '%s', %f 
         WHERE NOT EXISTS (
           SELECT 1 
           FROM Retornos_Historicos 
           WHERE ticker_id = %d AND data = '%s'
         )", 
        ticker_id, data_str, retorno,
        ticker_id, data_str
      )
      
      dbExecute(con, query)
    }
    
    message(sprintf("%d retornos diários inseridos (ignorando duplicatas).", nrow(dados)))
  } else {
    message("Nenhum retorno diário para inserir.")
  }
}


#' @title Inserir Preços Históricos
#' @description Insere preços históricos na tabela Precos_Historicos, evitando duplicatas.
#' @param con Objeto de conexão com o banco de dados.
#' @param dados Data frame contendo os dados a serem inseridos.
inserir_precos_historicos <- function(con, dados) {
  if (!is.null(dados) && nrow(dados) > 0) {
    for (i in 1:nrow(dados)) {
      ticker_id <- dados$ticker_id[i]
      data_str <- dados$data[i]
      preco <- dados$preco_fechamento_ajustado[i]
      
      query <- sprintf(
        "INSERT INTO Precos_Historicos (ticker_id, data, preco_fechamento_ajustado) 
         SELECT %d, '%s', %f 
         WHERE NOT EXISTS (
           SELECT 1 
           FROM Precos_Historicos 
           WHERE ticker_id = %d AND data = '%s'
         )", 
        ticker_id, data_str, preco,
        ticker_id, data_str
      )
      
      dbExecute(con, query)
    }
    
    message(sprintf("%d preços históricos inseridos (ignorando duplicatas).", nrow(dados)))
  } else {
    message("Nenhum preço histórico para inserir.")
  }
}
