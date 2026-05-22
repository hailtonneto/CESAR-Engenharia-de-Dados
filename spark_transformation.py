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
MYSQL_URI = os.getenv("MYSQL_URI")


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
        .getOrCreate()
    )


def ler_mongodb(spark: SparkSession):
    print("Lendo dados do MongoDB Atlas...")
    df = spark.read.format("mongodb").load()
    print(f"Total de documentos lidos: {df.count()}")
    return df


def transformar(df):
    print("Transformando dados...")

    colunas = [c for c in [
        "numeroCompra",
        "anoCompra",
        "objetoCompra",
        "valorTotalEstimado",
        "dataAtualizacao",
        "uf",
        "municipioNome",
        "run_id",
    ] if c in df.columns]

    df = df.select(colunas)

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
        salvar_mysql(df_limpo, nome_tabela="editais_recife_spark")
        print("Pipeline PySpark concluído com sucesso!")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
