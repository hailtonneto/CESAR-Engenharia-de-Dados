from sqlalchemy import create_engine, text
from pyspark.sql.functions import col, when, to_timestamp, round as spark_round
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import pandas as pd
import os
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]


load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
MONGO_COLLECTION_LIMPAS = os.getenv("MONGO_COLLECTION_LIMPAS", "contratacoes_limpas")
MYSQL_URI = os.getenv("MYSQL_URI")

# Colunas escalares que compõem o schema relacional do MySQL. Os demais
# campos (inclusive os structs aninhados) são preservados e gravados na
# coleção limpa do MongoDB.
COLUNAS_MYSQL = [
    "numeroCompra",
    "anoCompra",
    "objetoCompra",
    "valorTotalEstimado",
    "dataAtualizacao",
    "uf",
    "municipioNome",
    "run_id",
    "faixaValor",
]


def criar_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ETL PNCP — PySpark Transformation")
        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
        )
        .config("spark.mongodb.read.connection.uri", MONGO_URI)
        .config("spark.mongodb.read.database",       MONGO_DB_NAME)
        .config("spark.mongodb.read.collection",     MONGO_COLLECTION)
        .config("spark.mongodb.read.ssl.invalidHostNameAllowed", "true")
        .config("spark.mongodb.read.tls.insecure",   "true")
        # Configuração de escrita para a coleção limpa do MongoDB.
        .config("spark.mongodb.write.connection.uri", MONGO_URI)
        .config("spark.mongodb.write.database",       MONGO_DB_NAME)
        .config("spark.mongodb.write.collection",     MONGO_COLLECTION_LIMPAS)
        .config("spark.mongodb.write.ssl.invalidHostNameAllowed", "true")
        .config("spark.mongodb.write.tls.insecure",   "true")
        .getOrCreate()
    )


def ler_mongodb(spark: SparkSession):
    print("Lendo dados do MongoDB Atlas...")
    df = spark.read.format("mongodb").load()
    print(f"Total de documentos lidos: {df.count()}")
    return df


def transformar(df):
    """
    Higieniza os dados preservando TODOS os atributos do documento original,
    inclusive os structs aninhados (``orgaoEntidade``, ``unidadeOrgao``,
    ``amparoLegal``) e os links oficiais (``linksOficiais``,
    ``linkSistemaOrigem``, ``linkProcessoEletronico``, ``linkPncp``/
    ``linkPNCP``).

    Diferente da versão anterior, NÃO restringimos as colunas via ``select``;
    apenas adicionamos/normalizamos campos derivados. Assim o DataFrame
    completo segue para a coleção limpa do MongoDB, e a seleção das colunas
    escalares para o MySQL acontece somente na carga relacional.
    """
    print("Transformando dados...")

    # Normaliza tipos e valores sem descartar nenhuma coluna.
    if "valorTotalEstimado" in df.columns:
        df = df.withColumn(
            "valorTotalEstimado",
            spark_round(col("valorTotalEstimado").cast("double"), 2)
        )

    if "dataAtualizacao" in df.columns:
        df = df.withColumn(
            "dataAtualizacao",
            to_timestamp(col("dataAtualizacao"))
        )

    if "anoCompra" in df.columns:
        df = df.withColumn("anoCompra", col("anoCompra").cast("int"))

    # Deriva municipioNome/codigoIbge a partir do struct unidadeOrgao,
    # caso ainda não estejam achatados no topo do documento.
    if "unidadeOrgao" in df.columns:
        if "municipioNome" not in df.columns:
            df = df.withColumn("municipioNome", col("unidadeOrgao.municipioNome"))
        if "codigoIbge" not in df.columns:
            df = df.withColumn("codigoIbge", col("unidadeOrgao.codigoIbge"))

    if "valorTotalEstimado" in df.columns:
        df = df.withColumn(
            "faixaValor",
            when(col("valorTotalEstimado") < 10000, "Até 10k")
            .when(col("valorTotalEstimado") < 30000, "10k–30k")
            .when(col("valorTotalEstimado") < 60000, "30k–60k")
            .otherwise("60k–81k")
        )

    print(f"Registros após transformação: {df.count()}")
    df.show(5, truncate=True)
    return df


def salvar_mongodb_limpos(df, collection: str = MONGO_COLLECTION_LIMPAS):
    """
    Espelha a carga limpa na coleção ``contratacoes_limpas`` do MongoDB,
    preservando todos os atributos (incluindo os structs aninhados).

    Usa o conector oficial do Spark para MongoDB em modo ``append`` (upsert
    por documento já tratado a montante via ``numeroCompra``).
    """
    print(f"Salvando dados limpos no MongoDB — coleção '{collection}'...")
    (
        df.write
        .format("mongodb")
        .mode("append")
        .option("collection", collection)
        .option("idFieldList", "numeroCompra")
        .option("operationType", "update")
        .save()
    )
    print("Dados limpos sincronizados com o MongoDB Atlas.")


def garantir_tabela(engine, nome_tabela: str):
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS `{nome_tabela}` (
                numeroCompra       VARCHAR(100) PRIMARY KEY,
                anoCompra          INT,
                objetoCompra       TEXT,
                valorTotalEstimado DOUBLE,
                dataAtualizacao    DATETIME,
                uf                 VARCHAR(10),
                municipioNome      VARCHAR(100),
                run_id             VARCHAR(100),
                faixaValor         VARCHAR(20)
            )
        """))


def salvar_mysql(df, nome_tabela: str = "editais_recife_spark"):
    print(f"Salvando no MySQL — tabela '{nome_tabela}'...")

    import math

    # O DataFrame transformado preserva os structs aninhados (para o
    # MongoDB). Para o MySQL selecionamos apenas as colunas escalares do
    # schema relacional, evitando tentar serializar dicts/structs.
    colunas_relacionais = [c for c in COLUNAS_MYSQL if c in df.columns]
    df = df.select(colunas_relacionais)

    pandas_df = df.toPandas()

    registros = []
    for row in pandas_df.to_dict(orient="records"):
        limpo = {
            k: (None if (v is not None and isinstance(v, float) and math.isnan(v)) else v)
            for k, v in row.items()
        }
        registros.append(limpo)

    registros = [r for r in registros if r.get("numeroCompra") is not None]
    print(f"Registros válidos (com numeroCompra): {len(registros)}")

    engine = create_engine(MYSQL_URI)
    garantir_tabela(engine, nome_tabela)

    colunas = list(pandas_df.columns)
    placeholders = ", ".join([f":{c}" for c in colunas])
    updates = ", ".join([f"`{c}` = VALUES(`{c}`)" for c in colunas if c != "numeroCompra"])

    sql = text(f"""
        INSERT INTO `{nome_tabela}` ({', '.join(f'`{c}`' for c in colunas)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {updates}
    """)

    with engine.begin() as conn:
        conn.execute(sql, registros)

    print(f"{len(registros)} linhas sincronizadas na tabela '{nome_tabela}'. Nenhum dado apagado.")


def main():
    spark = criar_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        df_bruto = ler_mongodb(spark)
        df_limpo = transformar(df_bruto)
        # Camada limpa no MongoDB (com structs aninhados, consumida pela API).
        salvar_mongodb_limpos(df_limpo, collection=MONGO_COLLECTION_LIMPAS)
        # Camada gold relacional no MySQL (apenas colunas escalares).
        salvar_mysql(df_limpo, nome_tabela="editais_recife_spark")
        print("Pipeline PySpark concluído com sucesso!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
