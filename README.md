# Projeto ETL - Dados do PNCP (Recife)

## 📌 Proposta
Este projeto realiza a extração, transformação e carga (ETL) de dados de contratações públicas da cidade de Recife, consumindo a API do Portal Nacional de Contratações Públicas (PNCP). O objetivo é aplicar as melhores práticas de Engenharia de Dados.

## 🏗️ Arquitetura da Solução
A solução foi desenvolvida em Python, seguindo o paradigma de Orientação a Objetos. O fluxo consiste em:
1. **Extração:** Consumo da API do PNCP (via `requests`).
2. **Transformação:** Limpeza e seleção de colunas relevantes utilizando `pandas`.
3. **Carga:** Persistência dupla. Os dados brutos são enviados para o **MongoDB Atlas** (NoSQL), enquanto os dados estruturados são carregados em um banco **MySQL** (Relacional).

## ⚙️ Forma de Execução

1. Crie o banco de dados no MySQL Workbench:
   `CREATE DATABASE pncp_recife;`
2. Instale as dependências:
   `pip install -r requirements.txt`
3. Configure o arquivo `.env` com as suas credenciais (`MONGO_URI` e `MYSQL_URI`).
4. Execute o pipeline:
   `python main.py`