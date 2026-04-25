import os
import logging
import pandas as pd
from datetime import datetime, timezone
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pymongo import MongoClient
from pymongo.errors import PyMongoError

load_dotenv()
logger = logging.getLogger("etl.database")


class DatabaseConnector:

    def __init__(self):
        self.mongo_uri = os.getenv("MONGO_URI")
        self.mysql_uri = os.getenv("MYSQL_URI")
        self.mongo_db_name = os.getenv("MONGO_DB_NAME", "pncp_etl")
        self.mongo_collection_name = os.getenv("MONGO_COLLECTION", "contratacoes_brutas")

        if not self.mongo_uri:
            raise EnvironmentError(
                "Variável MONGO_URI não encontrada no .env. "
                "Consulte o README para configurar o MongoDB Atlas."
            )
        if not self.mysql_uri:
            raise EnvironmentError(
                "Variável MYSQL_URI não encontrada no .env."
            )

    def carregar_mongodb(self, dados_brutos: list[dict], run_id: str | None = None) -> None:
    
        if not dados_brutos:
            logger.warning("MongoDB: nenhum dado bruto para inserir.")
            return

        try:
            cliente = MongoClient(self.mongo_uri, serverSelectionTimeoutMS=10_000)
            db = cliente[self.mongo_db_name]
            colecao = db[self.mongo_collection_name]

            timestamp_atual = datetime.now(timezone.utc).isoformat()
            documentos = [
                {**doc, "_run_id": run_id, "_ingestao_em": timestamp_atual}
                for doc in dados_brutos
            ]

            resultado = colecao.insert_many(documentos)
            logger.info(
                f"MongoDB Atlas: {len(resultado.inserted_ids)} documentos inseridos "
                f"em '{self.mongo_db_name}.{self.mongo_collection_name}'."
            )

        except PyMongoError as e:
            logger.error(f"Falha ao inserir no MongoDB: {e}")
            raise
        finally:
            cliente.close()

    def carregar_mysql(
        self, df: pd.DataFrame, nome_tabela: str = "editais_recife"
    ) -> None:
        
        if df.empty:
            logger.warning("MySQL: DataFrame vazio, nada para carregar.")
            return

        try:
            engine = create_engine(self.mysql_uri)
            df.to_sql(nome_tabela, con=engine, if_exists="append", index=False)
            logger.info(
                f"MySQL: {len(df)} linhas inseridas na tabela '{nome_tabela}'."
            )
        except Exception as e:
            logger.error(f"Falha ao carregar no MySQL: {e}")
            raise