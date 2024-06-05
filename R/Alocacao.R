# Carrega os pacotes necessários
library(quantmod)
library(PerformanceAnalytics)
library(ggplot2)
library(readr)

# Define a função para calcular a fronteira eficiente
calcular_fronteira_eficiente <- function(retornos, cov_matrix) {
  # Calcula os retornos esperados e desvio padrão da carteira
  port_returns <- seq(min(colMeans(retornos)), max(colMeans(retornos)), length.out = 100)
  port_sd <- sapply(port_returns, function(r) {
    w <- solve(cov_matrix, rep(1, ncol(cov_matrix))) * (r - risk_free_rate)
    w <- w / sum(w)
    sqrt(t(w) %*% cov_matrix %*% w)
  })
  
  buscar_precos <- function(ativos) {
    precos <- getQuote(ativos, src = "yahoo")$Last
    names(precos) <- ativos
    return(precos)
  }
  
  
  # Cria um data frame com os resultados
  data.frame(Retorno = port_returns, Desvio_Padrão = port_sd)
}

# Define a taxa livre de risco
risk_free_rate <- 0.05 / 252

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

precos_atuais <- buscar_precos(ativos)
carteira$Preco <- precos_atuais[carteira$codigo]

# Calcula o valor total da carteira
carteira$Valor <- carteira$quantidade * carteira$Preco
valor_total <- sum(carteira$Valor)

# Calcula o percentual de cada ativo na carteira
carteira$Percentual <- carteira$Valor / valor_total

# Define o valor do aporte
valor_aporte <- 10000

# Calcula a quantidade de cada ativo a ser comprada
carteira$Quantidade_Aporte <- (valor_aporte * carteira$Percentual) / carteira$Preco

# Imprime os resultados
print(carteira)

# Plota a fronteira eficiente
ggplot(fronteira_eficiente, aes(x = Desvio_Padrão, y = Retorno)) +
  geom_line() +
  labs(title = "Fronteira Eficiente de Markowitz",
       x = "Desvio Padrão",
       y = "Retorno Esperado")
