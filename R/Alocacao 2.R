# Carrega os pacotes necessários
library(quantmod)
library(PerformanceAnalytics)
library(ggplot2)
library(readr)
library(quadprog)

#Buscar taxa selic
buscar_taxa_selic_atual <- function() {
  
  # URL da API do Banco Central para a série da Selic diária
  url <- "https://api.bcb.gov.br/dados/serie/bcdata.sgs.11/dados?formato=json"
  
  # Faz a requisição para a API
  dados_selic <- jsonlite::fromJSON(url)
  
  # Converte a coluna 'data' para o formato Date
  dados_selic$data <- as.Date(dados_selic$data, format = "%d/%m/%Y")
  
  # Encontra a data mais recente disponível
  data_mais_recente <- max(dados_selic$data)
  
  # Filtra os dados para a data mais recente
  selic_atual <- dados_selic[dados_selic$data == data_mais_recente, "valor"]
  
  # Retorna a taxa Selic atual como um número decimal
  return(as.numeric(selic_atual) / 100)
}


# Define a função para calcular a fronteira eficiente
calcular_fronteira_eficiente <- function(retornos, cov_matrix) {
  # Calcula os retornos esperados e desvio padrão da carteira
  port_returns <- seq(min(colMeans(retornos)), max(colMeans(retornos)), length.out = 100)
  port_sd <- sapply(port_returns, function(r) {
    w <- solve(cov_matrix, rep(1, ncol(cov_matrix))) * (r - risk_free_rate)
    w <- w / sum(w)
    sqrt(t(w) %*% cov_matrix %*% w)
  })
  
  # Cria um data frame com os resultados
  data.frame(Retorno = port_returns, Desvio_Padrão = port_sd)
}

# Define a função para buscar os preços
buscar_precos <- function(ativos) {
  precos <- getQuote(ativos, src = "yahoo")$Last
  names(precos) <- ativos
  return(precos)
}

# Define a função para calcular a alocação ótima do aporte
# Corrigida: A função agora só compra ativos e garante que o aporte seja totalmente alocado.
calcular_alocação_aporte <- function(valor_atual, alocação_desejada, valor_aporte) {
  alocação_atual <- valor_atual / sum(valor_atual)
  
  # Calcula a alocação alvo considerando o aporte
  alocação_alvo <- (valor_atual + alocação_desejada * valor_aporte) / (sum(valor_atual) + valor_aporte)
  
  # Calcula a diferença entre a alocação alvo e atual
  diferença_alocação <- alocação_alvo - alocação_atual
  
  # Aporte é alocado proporcionalmente à diferença entre alocação alvo e atual
  alocação_aporte <- (diferença_alocação / sum(diferença_alocação[diferença_alocação > 0])) * valor_aporte
  
  # Ajusta para que a alocação seja apenas em compras
  alocação_aporte[alocação_aporte < 0] <- 0
  
  return(alocação_aporte / sum(alocação_aporte)) # Normaliza para garantir a soma 1
}

# Define a função para calcular a alocação ótima
calcular_alocação_otima <- function(retornos, cov_matrix, retorno_desejado) {
  # Define os parâmetros para o otimizador
  n <- ncol(retornos)
  Dmat <- 2 * cov_matrix
  dvec <- rep(0, n)
  Amat <- cbind(rep(1, n), colMeans(retornos))
  bvec <- c(1, retorno_desejado)
  
  # Resolve o problema de otimização
  sol <- solve.QP(Dmat, dvec, Amat, bvec, meq = 2)
  
  # Retorna a alocação ótima
  return(sol$solution)
}

# Define a taxa livre de risco
#risk_free_rate <- 0.05 / 252
risk_free_rate_anual <- 0.1375
risk_free_rate <- (1 + risk_free_rate_anual)^(1/252) - 1

# Define o caminho para o arquivo da carteira
caminho_carteira <- "data-raw/Carteira.csv"

# Importa a carteira de ativos
carteira <- read_csv2(caminho_carteira)

# Extrai os códigos dos ativos
ativos <- carteira$codigo

# Baixa os dados históricos de preços dos ativos 
getSymbols(ativos, src = "yahoo", from = Sys.Date() - 365, auto.assign = TRUE)

# Combina os preços ajustados em um único objeto xts
precos <- do.call(merge, lapply(ativos, function(x) Ad(get(x))))
colnames(precos) <- ativos # Define os nomes das colunas

# Converter precos para xts explicitamente
precos <- xts::xts(precos, order.by = index(precos))

# Calcula os retornos diários dos ativos
retornos <- na.omit(Return.calculate(precos))

# Calcula a matriz de covariância dos retornos
cov_matrix <- cov(retornos)

# Calcula a fronteira eficiente
fronteira_eficiente <- calcular_fronteira_eficiente(retornos, cov_matrix)

# Buscar os preços atuais dos ativos
precos_atuais <- buscar_precos(ativos)

# Adicionar a coluna Preco ao dataframe carteira
carteira$Preco <- precos_atuais[carteira$codigo]

# Calcula o valor total da carteira
carteira$Valor <- carteira$quantidade * carteira$Preco
valor_total <- sum(carteira$Valor)

# Define o valor do aporte
valor_aporte <- 1223

# Define o retorno desejado para o aporte
retorno_desejado <- 0.1 # Substitua por seu retorno desejado

# Calcula a alocação ótima
alocação_otima <- calcular_alocação_otima(retornos, cov_matrix, retorno_desejado)

# Calcula a alocação do novo aporte
alocação_aporte <- calcular_alocação_aporte(carteira$Valor, alocação_otima, valor_aporte)

# Calcula a quantidade a aportar em cada ativo
carteira$Aporte <- alocação_aporte * valor_aporte / carteira$Preco

# Calcula a quantidade de ações a serem compradas
carteira$Acoes_a_Comprar <- floor(carteira$Aporte) 

# Imprime os resultados
print(carteira)

# Plota a fronteira eficiente
ggplot(fronteira_eficiente, aes(x = Desvio_Padrão, y = Retorno)) +
  geom_line() +
  labs(title = "Fronteira Eficiente de Markowitz",
       x = "Desvio Padrão",
       y = "Retorno Esperado") 


writexl::write_xlsx(carteira, "data-raw/Resultado.xlsx")
