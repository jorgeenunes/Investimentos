# Arquivo: tests/test-stock_data_import.R
#library(Investimentos)
library(xts)
library(RSQLite)
library(quantmod)
library(dplyr)
library(lubridate)
library(R.utils)
library(bizdays)
library(zoo)
library(testthat)

source("R/Funcoes.R")

# Conectar a um banco de dados em memória para testes
con <- dbConnect(RSQLite::SQLite(), ":memory:")

# Criar as tabelas de teste
dbExecute(con, "CREATE TABLE Ativos (id INTEGER PRIMARY KEY, ticker TEXT NOT NULL UNIQUE)")
dbExecute(con, "CREATE TABLE Precos_Historicos (id INTEGER PRIMARY KEY, ticker_id INTEGER, data DATE, preco_fechamento_ajustado REAL)")
dbExecute(con, "CREATE TABLE Retornos_Historicos (id INTEGER PRIMARY KEY, ticker_id INTEGER, data DATE, retorno_diario REAL)")

# Testes para formatar_dados_precos
test_that("Formatar dados de preços - Entrada válida", {
  dados <- xts::xts(cbind(Close = c(10, 11, 12), Adjusted = c(9.8, 10.8, 11.8)), 
                    as.Date(c("2023-01-01", "2023-01-02", "2023-01-03")))
  resultado <- formatar_dados_precos(dados, 1)
  expect_equal(nrow(resultado), 3)
  expect_equal(resultado$ticker_id, c(1, 1, 1))
  # Correção: data esperada agora corresponde à data de entrada
  expect_equal(resultado$data, c("2023-01-01", "2023-01-02", "2023-01-03"))
  expect_equal(resultado$preco_fechamento_ajustado, c(9.8, 10.8, 11.8)) 
})

test_that("Formatar dados de preços - Entrada NULL", {
  resultado <- formatar_dados_precos(NULL, 1)
  expect_null(resultado)
})

# # Testes para inserir_dados
# test_that("Inserir dados - Sem dados duplicados", {
#   dados <- data.frame(ticker_id = 1, data = as.Date("2023-01-02"), preco_fechamento_ajustado = 10)
#   inserir_dados(con, "Precos_Historicos", dados)
#   resultado <- dbGetQuery(con, "SELECT COUNT(*) FROM Precos_Historicos")
#   expect_equal(resultado[1, 1], 1)
# })
# 
# test_that("Inserir dados - Com dados duplicados", {
#   dados <- data.frame(ticker_id = 1, data = as.Date("2023-01-02"), preco_fechamento_ajustado = 10)
#   inserir_dados(con, "Precos_Historicos", dados)
#   resultado <- dbGetQuery(con, "SELECT COUNT(*) FROM Precos_Historicos")
#   expect_equal(resultado[1, 1], 1)
# })

# Testes para inserir_precos_historicos
test_that("inserir_precos_historicos - Sem dados duplicados", {
  dados <- data.frame(ticker_id = 1, data = as.Date("2023-01-02"), preco_fechamento_ajustado = 10)
  inserir_precos_historicos(con, dados)
  resultado <- dbGetQuery(con, "SELECT COUNT(*) FROM Precos_Historicos")
  expect_equal(resultado[1, 1], 1)
})

test_that("inserir_precos_historicos - Com dados duplicados", {
  dados <- data.frame(ticker_id = 1, data = as.Date("2023-01-02"), preco_fechamento_ajustado = 10)
  inserir_precos_historicos(con, dados)
  inserir_precos_historicos(con, dados) # Tentando inserir novamente
  resultado <- dbGetQuery(con, "SELECT COUNT(*) FROM Precos_Historicos")
  expect_equal(resultado[1, 1], 1) # Verificando se ainda temos apenas 1 registro
})

# Testes para inserir_retornos_diarios
test_that("inserir_retornos_diarios - Sem dados duplicados", {
  dados <- data.frame(ticker_id = 1, data = as.Date("2023-01-03"), retorno_diario = 0.05)
  inserir_retornos_diarios(con, dados)
  resultado <- dbGetQuery(con, "SELECT COUNT(*) FROM Retornos_Historicos")
  expect_equal(resultado[1, 1], 1)
})

test_that("inserir_retornos_diarios - Com dados duplicados", {
  dados <- data.frame(ticker_id = 1, data = as.Date("2023-01-03"), retorno_diario = 0.05)
  inserir_retornos_diarios(con, dados)
  inserir_retornos_diarios(con, dados) # Tentando inserir novamente
  resultado <- dbGetQuery(con, "SELECT COUNT(*) FROM Retornos_Historicos")
  expect_equal(resultado[1, 1], 1) # Verificando se ainda temos apenas 1 registro
})

# Desconectar do banco de dados de teste
dbDisconnect(con)
