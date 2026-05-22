import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pymongo import MongoClient, UpdateOne

load_dotenv()

class DatabaseConnector:
    def __init__(self):
        self.mysql_uri = os.getenv("MYSQL_URI")
        self.mongo_uri = os.getenv("MONGO_URI")
        self.mongo_db_name = os.getenv("MONGO_DB_NAME")
        self.mongo_collection = os.getenv("MONGO_COLLECTION")

    def carregar_mysql(self, df: pd.DataFrame, nome_tabela: str = "editais_recife"):
        if not df.empty:
            engine = create_engine(self.mysql_uri)
            df.to_sql(nome_tabela, con=engine, if_exists='replace', index=False)

    def carregar_mongodb(self, dados: list, run_id: str = ""):
        if not dados:
            print("Aviso: Lista de dados para MongoDB está vazia. Nada para salvar.")
            return
        
        try:
            client = MongoClient(self.mongo_uri, tlsAllowInvalidCertificates=True)
            db = client[self.mongo_db_name]
            collection = db[self.mongo_collection]

            operacoes = []
            for item in dados:
                if run_id:
                    item["run_id"] = run_id
                operacoes.append(
                    UpdateOne(
                        {"numeroCompra": item.get("numeroCompra")},
                        {"$set": item},
                        upsert=True
                    )
                )

            print("Sincronizando dados com MongoDB...")
            resultado = collection.bulk_write(operacoes)
            print(f"Inseridos: {resultado.upserted_count} | Modificados: {resultado.modified_count}")
            client.close()
        except Exception as e:
            print(f"Erro ao conectar ou inserir dados no MongoDB: {e}")