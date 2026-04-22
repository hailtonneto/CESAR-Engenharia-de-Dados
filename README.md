# 🚀 Projeto ETL — Dados do PNCP (Recife)

## 📌 Visão Geral
Este projeto tem como objetivo implementar um pipeline de **ETL (Extract, Transform, Load)** para dados de contratações públicas da cidade do Recife, utilizando a API do **Portal Nacional de Contratações Públicas (PNCP)**.

A proposta foca na aplicação de boas práticas de **Engenharia de Dados**, incluindo organização de código, separação de responsabilidades e persistência em diferentes tipos de banco de dados.

---

## 🏗️ Arquitetura da Solução

A aplicação foi desenvolvida em **Python**, seguindo o paradigma de **Programação Orientada a Objetos (POO)**.

O pipeline é dividido em três etapas principais:

### 🔹 1. Extração
- Consumo da API do PNCP utilizando a biblioteca `requests`
- Coleta de dados brutos de contratações públicas

### 🔹 2. Transformação
- Limpeza e tratamento dos dados com `pandas`
- Seleção e padronização das colunas relevantes

### 🔹 3. Carga
- **MongoDB Atlas (NoSQL):** armazenamento dos dados brutos  
- **MySQL (Relacional):** armazenamento dos dados estruturados  

---

## 🛠️ Tecnologias Utilizadas

- Python  
- Pandas  
- Requests  
- MongoDB Atlas  
- MySQL  
- Python-dotenv  

---

## ⚙️ Como Executar o Projeto

### 1. Clone o repositório
```bash
git clone <url-do-repositorio>
cd <nome-do-projeto>
```

### 2. Crie o banco de dados no MySQL

```sql
CREATE DATABASE pncp_recife;
```

### 3. Instale as dependências

```bash
pip install -r requirements.txt
```

### 4. Configure as variáveis de ambiente

Crie um arquivo `.env` na raiz do projeto com:

```env
MONGO_URI=sua_string_de_conexao_mongodb
MYSQL_URI=sua_string_de_conexao_mysql
```

### 5. Execute o pipeline

```bash
python main.py
```

---

## 📊 Estrutura do Projeto (exemplo)

```
├── data/
├── src/
│   ├── extract/
│   ├── transform/
│   ├── load/
├── main.py
├── requirements.txt
└── .env
```

---

## 👥 Participantes

* Caio Furtado
* Emanuel Henry
* Hailton Neto
* Milo Castro
* Sofia Travassos
* Vinícius Souza

---

## 🎯 Objetivo Acadêmico

Este projeto foi desenvolvido com fins educacionais, com foco na prática de conceitos essenciais de Engenharia de Dados, como:

* Construção de pipelines
* Integração com APIs
* Modelagem de dados
* Persistência híbrida (SQL + NoSQL)