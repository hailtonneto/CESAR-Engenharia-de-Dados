# Implementação — Preservação de campos aninhados e camada limpa no MongoDB

## 1. O problema

O pipeline ETL (Prefect + Pandas + PySpark) extrai contratações públicas do
PNCP, salva os documentos brutos no MongoDB (coleção `contratacoes_brutas`) e,
em seguida, transforma os dados.

A etapa de transformação (`src/transformation.py`, classe `TransformadorDados`)
**descartava colunas aninhadas cruciais** para o frontend/API. A versão antiga
restringia o resultado a uma lista pequena de colunas escalares:

```python
colunas_interesse = ['numeroCompra','anoCompra','objetoCompra',
                     'valorTotalEstimado','dataAtualizacao','uf','municipioNome']
df_limpo = df[colunas_presentes]   # tudo o mais era perdido
```

Com isso, objetos como `orgaoEntidade`, `unidadeOrgao`, `amparoLegal` e os links
oficiais (`linksOficiais`, `linkSistemaOrigem`, `linkProcessoEletronico`,
`linkPncp`) eram jogados fora. Além disso, os dados limpos eram gravados **apenas
no MySQL** — não havia uma coleção limpa no MongoDB, que é a fonte consumida pela
API backend.

## 2. A solução (visão geral)

1. A transformação passa a **preservar todos os campos** (escalares e
   dicionários aninhados).
2. Os dados limpos passam a ser gravados também no MongoDB, na nova coleção
   **`contratacoes_limpas`**, além do MySQL.
3. O MySQL continua recebendo apenas as colunas escalares (seu schema relacional
   não comporta dicionários aninhados).

### Novo fluxo de dados

```
PNCP API
   │  (extração)
   ▼
contratacoes_brutas  (MongoDB, camada bronze/raw — documento completo)
   │  (transformação: filtro por valor + normalização de
   │   municipioNome/codigoIbge/uf, PRESERVANDO os dicts aninhados)
   ▼
registros limpos (list[dict] com TODOS os campos)
   ├─────────────► contratacoes_limpas  (MongoDB — consumido pela API/frontend)
   └─────────────► editais_recife       (MySQL — apenas colunas escalares)
```

## 3. O que foi alterado em cada arquivo

### `src/transformation.py`
Reescrito para expor **dois pontos de saída** que compartilham a mesma
higienização (`_higienizar`):

- **`transformar_registros(dados, valor_maximo, uf) -> list[dict]`** — NOVO.
  Retorna a lista de registros com **todos os campos preservados**, incluindo os
  dicionários aninhados (`orgaoEntidade`, `unidadeOrgao`, `amparoLegal`,
  `linksOficiais`, etc.). É essa saída que alimenta o MongoDB (`contratacoes_limpas`).
- **`transformar(dados, valor_maximo, uf) -> pd.DataFrame`** — mantido para
  compatibilidade. Agora reaproveita `_higienizar` e devolve um DataFrame
  achatado contendo apenas as **colunas escalares** (lista `COLUNAS_ESCALARES`),
  usado na carga relacional do MySQL.

A higienização (`_higienizar`):
- Filtra por valor (`valorTotalEstimado` presente e, se houver limite,
  `< valor_maximo`).
- Normaliza `municipioNome` e `codigoIbge` a partir de `unidadeOrgao`, **sem
  remover** o dicionário `unidadeOrgao` original.
- Garante o campo `uf` no topo do documento (usa `unidadeOrgao.ufSigla` ou o
  parâmetro `uf` quando ausente).
- Unifica a grafia do link PNCP (`linkPNCP` → também disponibiliza `linkPncp`).

Constantes adicionadas:
- `COLUNAS_ESCALARES` — colunas escalares preservadas (numeroCompra, anoCompra,
  objetoCompra, valorTotalEstimado, modalidadeNome, modoDisputaNome,
  situacaoCompraNome, informacaoComplementar, datas de abertura/encerramento/
  publicação/atualização, linkSistemaOrigem, linkProcessoEletronico, uf,
  municipioNome, codigoIbge).
- `COLUNAS_ANINHADAS` — referência dos dicionários preservados (`orgaoEntidade`,
  `unidadeOrgao`, `amparoLegal`, `linksOficiais`).

### `src/database.py`
- No `__init__`, lê a nova variável de ambiente
  `MONGO_COLLECTION_LIMPAS` (default `"contratacoes_limpas"`) em
  `self.mongo_collection_limpas`.
- Novo método **`carregar_mongodb_limpos(self, dados: list, run_id: str = "")`**,
  que espelha a lógica de `carregar_mongodb`: usa `MongoClient` com
  `tlsAllowInvalidCertificates=True`, monta `UpdateOne` com `upsert=True` por
  `numeroCompra` e executa `bulk_write`, gravando na coleção limpa. Inclui os
  mesmos logs (sincronização, inseridos/modificados, tratamento de erro).

### `orchestrate_prefect.py`
- `task_transformar` agora chama `transformador.transformar_registros(...)` e
  retorna a lista de dicts com os campos preservados (em vez de
  `df.to_dict(...)`).
- Nova task **`task_carregar_mongodb_limpos(registros, run_id)`**, que chama
  `DatabaseConnector.carregar_mongodb_limpos(...)`.
- `task_carregar_mysql` passou a selecionar apenas as colunas escalares
  (`TransformadorDados.COLUNAS_ESCALARES`) antes de gravar no MySQL, já que os
  registros agora trazem também os dicionários aninhados.
- O flow `etl_pncp_flow` mantém a assinatura e a compatibilidade; agora chama,
  após a transformação, **tanto** `task_carregar_mongodb_limpos` **quanto**
  `task_carregar_mysql`.

### `spark_transformation.py`
- Lê a variável `MONGO_COLLECTION_LIMPAS` e adiciona configs de **escrita** do
  conector MongoDB do Spark (`spark.mongodb.write.*`).
- `transformar(df)` deixou de fazer `select` de colunas: agora **preserva todos
  os atributos** (incluindo os structs aninhados) e apenas adiciona/normaliza
  campos derivados (`valorTotalEstimado`, `dataAtualizacao`, `anoCompra`,
  `municipioNome`/`codigoIbge` derivados de `unidadeOrgao`, e `faixaValor`).
- Novo `salvar_mongodb_limpos(df, collection)` que grava o DataFrame completo na
  coleção `contratacoes_limpas` (modo `append` com upsert por `numeroCompra`).
- `salvar_mysql(df, ...)` agora seleciona apenas as colunas escalares
  (`COLUNAS_MYSQL`) antes de converter para Pandas e gravar no MySQL.
- `main()` chama `salvar_mongodb_limpos` (camada limpa Mongo) **e**
  `salvar_mysql` (camada gold relacional).

> Observação: o script PySpark não foi executado neste ambiente (sem Spark/
> credenciais), mas está sintaticamente correto e coerente.

### `.env`
Adicionada a variável:

```env
MONGO_COLLECTION_LIMPAS=contratacoes_limpas
```

### `requirements.txt`
Sem alteração — `pymongo`, `pandas` e `prefect` já constavam.

## 4. Novas variáveis de ambiente

| Variável                  | Default                | Descrição                                             |
| ------------------------- | ---------------------- | ----------------------------------------------------- |
| `MONGO_COLLECTION_LIMPAS` | `contratacoes_limpas`  | Coleção MongoDB onde são gravados os dados limpos     |

As demais (`MONGO_URI`, `MONGO_DB_NAME`, `MONGO_COLLECTION`, `MYSQL_URI`)
permanecem inalteradas.

## 5. Como rodar e validar

### Validar a sintaxe de todos os arquivos
```bash
python -c "import ast; \
ast.parse(open('src/transformation.py').read()); \
ast.parse(open('src/database.py').read()); \
ast.parse(open('orchestrate_prefect.py').read()); \
ast.parse(open('spark_transformation.py').read())"
```

### Executar o pipeline Prefect (requer dependências + credenciais)
```bash
pip install -r requirements.txt
python orchestrate_prefect.py
```
Isso executa `etl_pncp_flow`, que:
1. extrai do PNCP;
2. grava brutos em `contratacoes_brutas`;
3. transforma preservando os aninhados;
4. grava limpos em `contratacoes_limpas` (MongoDB) **e** em `editais_recife` (MySQL).

### Executar a versão PySpark (alternativa de transformação/carga)
```bash
python spark_transformation.py
```
Lê de `contratacoes_brutas`, preserva os structs aninhados e grava em
`contratacoes_limpas` (MongoDB) + `editais_recife_spark` (MySQL).

## 6. Resumo

- Nenhum campo é mais descartado na transformação.
- O backend/API pode ler `contratacoes_limpas` no MongoDB com os objetos
  aninhados completos (`orgaoEntidade`, `unidadeOrgao`, `amparoLegal`, links).
- O MySQL continua recebendo apenas as colunas escalares, mantendo o schema
  relacional intacto.

---

## PLANO-MVP — Execução (ETL)

Esta seção documenta as mudanças do PLANO-MVP relativas à execução do
pipeline: entrypoint correto, janela de datas móvel, agendamento, chave de
upsert robusta, streaming corrigido, testes e portabilidade.

### 1. Entrypoint correto (`main.py`) e flow canônico

Antes, `main.py` importava `src.pipeline.etl_pncp_flow`, que executa apenas a
versão REDUZIDA do fluxo (extração + carga de BRUTOS no MongoDB). A camada
limpa (`contratacoes_limpas`) e o MySQL nunca eram populados por `python
main.py`.

Mudanças:

- **`main.py`** passou a importar o fluxo COMPLETO de
  `orchestrate_prefect.py` (`from orchestrate_prefect import etl_pncp_flow`).
  Agora `python main.py` executa: extração → brutos → transformação
  (preservando aninhados) → `contratacoes_limpas` (MongoDB) → `editais_recife`
  (MySQL).
- **`orchestrate_prefect.py` é o flow CANÔNICO.**
- **`src/pipeline.py` foi marcado como LEGADO** (docstring no topo do módulo):
  permanece como helper de extração/carga bruta, mas não é o entrypoint.

### 2. Janela de datas MÓVEL (sem datas fixas)

As datas estavam HARDCODED (`20260101`/`20260530`, etc.). Foram substituídas
por uma janela RELATIVA calculada em runtime.

- Novo helper **`janela_datas(janela_dias=None)`** em
  `orchestrate_prefect.py`: retorna `(data_inicial, data_final)` no formato
  `YYYYMMDD`, indo dos últimos `N` dias até hoje.
- `N` vem da variável de ambiente **`ETL_JANELA_DIAS`** (default **30**) ou do
  parâmetro.
- A assinatura de `etl_pncp_flow(data_inicial=None, data_final=None, ...)`
  agora aceita `None`; quando ausentes, o flow calcula a janela móvel.
- `main.py`, o bloco `__main__` de `orchestrate_prefect.py` e `serve_prefect.py`
  chamam o flow SEM datas, usando a janela móvel.

### 3. Agendamento (schedule)

Foram adicionados DOIS mecanismos de recorrência (cron diário às **06:00**,
timezone `America/Recife`), compatíveis com Prefect 3.x:

- **`serve_prefect.py`** (mais simples): usa `etl_pncp_flow.serve(...)` com
  `schedules=[Cron("0 6 * * *", timezone="America/Recife")]`. O cron pode ser
  sobrescrito por `ETL_CRON`.

  ```bash
  pip install -r requirements.txt
  python serve_prefect.py    # processo fica em primeiro plano e dispara o flow todo dia 06:00
  ```

- **`prefect.yaml`** (deployment declarativo): define o deployment
  `etl-pncp-diario` com `schedules: [{ cron: "0 6 * * *", timezone:
  "America/Recife" }]`.

  ```bash
  pip install -r requirements.txt
  prefect deploy --all
  prefect worker start -p default-agent-pool
  ```

Em ambas as opções o flow não recebe datas fixas: a cada execução agendada a
janela móvel (`ETL_JANELA_DIAS`) é recalculada.

### 4. Chave de upsert ROBUSTA (composta)

A dedup era feita apenas por `numeroCompra`, causando colisões (vários
documentos com o mesmo `numeroCompra` se sobrescreviam, gerando registros
parciais).

- Novo helper **`montar_filtro_upsert(item)`** em `src/database.py`. Estratégia:
  1. **`numeroControlePNCP`** (identificador único do PNCP) quando existir —
     critério preferencial.
  2. Senão, chave COMPOSTA: **`orgaoEntidade.cnpj` + `anoCompra` +
     `numeroCompra`**.
  3. Fallback (sem CNPJ/ano): `numeroCompra` isolado, para não perder o
     registro.
- `carregar_mongodb` e `carregar_mongodb_limpos` usam o helper no filtro do
  `UpdateOne`.
- **`spark_transformation.py`** aplica o MESMO critério: cria a coluna
  estável `_upsert_key` (`coalesce(numeroControlePNCP, cnpj|ano|numeroCompra)`)
  e usa `idFieldList="_upsert_key"` no `df.write` (modo `append` + `update` +
  `upsertDocument`). A coluna `_upsert_key` não é gravada no MySQL (a carga
  relacional seleciona apenas `COLUNAS_MYSQL`).

### 5. Streaming em tempo real corrigido (`src/streaming.py`)

Antes o change stream observava a coleção de BRUTOS (`MONGO_COLLECTION`) e só
reagia a `insert`. Como o ETL grava via UPSERT, atualizações de documentos
existentes geram `update`/`replace` (não `insert`) e eram perdidas.

Mudanças:

- Passa a observar a coleção LIMPA: `MONGO_COLLECTION_LIMPAS` (default
  `contratacoes_limpas`).
- O `watch` agora filtra `operationType ∈ {insert, update, replace}` e usa
  **`full_document="updateLookup"`**, de modo que eventos de `update` também
  tragam o documento completo.
- Trata `fullDocument` ausente (sem quebrar) e mantém o broadcast via
  `ConnectionManager` para os clientes WebSocket (`/ws/notificacoes`),
  filtrando por `municipioNome == "RECIFE"`.

### 6. Testes (`tests/`)

Criada a suíte pytest cobrindo lógica PURA (sem rede/DB):

- `tests/test_upsert_key.py` — `montar_filtro_upsert` (preferência por
  `numeroControlePNCP`, chave composta, distinção de `numeroCompra` repetido
  em órgãos diferentes, fallbacks).
- `tests/test_transformation.py` — `TransformadorDados._higienizar` /
  `transformar_registros` (preservação dos aninhados, normalização de
  `municipioNome`/`codigoIbge`/`uf`, filtro por valor, unificação do link
  PNCP) e o achatamento de `transformar`.
- `tests/test_janela_datas.py` — `janela_datas` (janela relativa, leitura de
  `ETL_JANELA_DIAS`, default em valor inválido).
- `tests/conftest.py` — garante a raiz no `sys.path`.

Os testes usam `pytest.importorskip` com motivo claro: se `pandas`/`pymongo`/
`sqlalchemy`/`dotenv`/`prefect` não estiverem instalados, são pulados em vez
de quebrar a coleta.

`pytest` foi adicionado ao `requirements.txt`.

Como rodar:

```bash
pip install -r requirements.txt   # ou, no mínimo: pip install pytest pandas pymongo sqlalchemy python-dotenv prefect
python -m pytest
```

> Resultado nesta implementação: **23 testes passando** (em venv com
> pandas + pymongo + sqlalchemy + python-dotenv + prefect instalados).

### 7. Portabilidade e reprodutibilidade

- **`spark_transformation.py`**: removidos os paths Windows hardcoded
  (`C:\hadoop`). Novo helper **`configurar_hadoop()`** é cross-platform: em
  não-Windows é no-op; no Windows lê `HADOOP_HOME` do ambiente e adiciona
  `HADOOP_HOME/bin` ao `PATH` (apenas se a variável estiver definida).
- **`requirements.txt`**: versões PINADAS e coerentes (`requests`, `pandas`,
  `pymongo`, `sqlalchemy`, `mysql-connector-python`, `python-dotenv`,
  `fastapi`, `uvicorn`, `motor`, `prefect`, `pyspark`, `pytest`, e as deps de
  MCP/chatbot).
- **`.env`**: adicionada a variável `ETL_JANELA_DIAS=30` (sem expor segredos).

### 8. Novas variáveis de ambiente

| Variável          | Default       | Descrição                                                        |
| ----------------- | ------------- | ---------------------------------------------------------------- |
| `ETL_JANELA_DIAS` | `30`          | Tamanho (em dias) da janela de datas móvel da extração           |
| `ETL_CRON`        | `0 6 * * *`   | Cron usado por `serve_prefect.py` (agendamento)                  |
| `HADOOP_HOME`     | (vazio)       | Apenas Windows: raiz do Hadoop/winutils para o PySpark           |
