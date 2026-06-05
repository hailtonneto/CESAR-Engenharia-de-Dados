import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from pymongo import MongoClient, UpdateOne

load_dotenv()


def montar_filtro_upsert(item: dict) -> dict:
    """
    Monta o filtro (chave de upsert) usado nas cargas do MongoDB.

    A dedup anterior usava apenas ``numeroCompra``, o que causava colisões
    (vários documentos compartilham o mesmo ``numeroCompra`` e registros
    parciais sobrescreviam uns aos outros). Aqui usamos uma chave COMPOSTA
    estável:

    1. ``numeroControlePNCP`` — identificador único do PNCP, quando existir.
       É o critério preferencial e suficiente por si só.
    2. Caso ausente, compõe-se a chave por
       ``orgaoEntidade.cnpj`` + ``anoCompra`` + ``numeroCompra``
       (sequencial da compra dentro do órgão/ano).
    3. Como último recurso (documentos sem CNPJ e sem ano), recai-se em
       ``numeroCompra`` isolado para não quebrar a carga.

    Retorna o dicionário de filtro a ser passado ao ``UpdateOne``.
    """
    numero_controle = item.get("numeroControlePNCP")
    if numero_controle:
        return {"numeroControlePNCP": numero_controle}

    orgao = item.get("orgaoEntidade") or {}
    cnpj = orgao.get("cnpj")
    ano = item.get("anoCompra")
    numero = item.get("numeroCompra")

    if cnpj and ano is not None:
        return {
            "orgaoEntidade.cnpj": cnpj,
            "anoCompra": ano,
            "numeroCompra": numero,
        }

    # Fallback: melhor um filtro abrangente do que perder o registro.
    return {"numeroCompra": numero}


class DatabaseConnector:
    def __init__(self):
        self.mysql_uri = os.getenv("MYSQL_URI")
        self.mongo_uri = os.getenv("MONGO_URI")
        self.mongo_db_name = os.getenv("MONGO_DB_NAME")
        self.mongo_collection = os.getenv("MONGO_COLLECTION")
        self.mongo_collection_limpas = os.getenv(
            "MONGO_COLLECTION_LIMPAS", "contratacoes_limpas"
        )

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
                        montar_filtro_upsert(item),
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

    def carregar_mongodb_limpos(self, dados: list, run_id: str = ""):
        """
        Grava os dados já transformados/higienizados (com os campos
        aninhados preservados) na coleção ``contratacoes_limpas`` do MongoDB.

        Espelha a lógica de :meth:`carregar_mongodb`: usa ``bulk_write`` com
        ``UpdateOne``/upsert tendo como chave o filtro composto montado por
        :func:`montar_filtro_upsert` (``numeroControlePNCP`` ou
        ``cnpj``+``anoCompra``+``numeroCompra``), de modo que execuções
        repetidas atualizam os documentos em vez de duplicá-los. O nome da
        coleção vem da variável de ambiente ``MONGO_COLLECTION_LIMPAS``
        (default ``contratacoes_limpas``).
        """
        if not dados:
            print("Aviso: Lista de dados limpos para MongoDB está vazia. Nada para salvar.")
            return

        try:
            client = MongoClient(self.mongo_uri, tlsAllowInvalidCertificates=True)
            db = client[self.mongo_db_name]
            collection = db[self.mongo_collection_limpas]

            operacoes = []
            for item in dados:
                if run_id:
                    item["run_id"] = run_id
                operacoes.append(
                    UpdateOne(
                        montar_filtro_upsert(item),
                        {"$set": item},
                        upsert=True
                    )
                )

            print(f"Sincronizando dados limpos com MongoDB (coleção '{self.mongo_collection_limpas}')...")
            resultado = collection.bulk_write(operacoes)
            print(f"Inseridos: {resultado.upserted_count} | Modificados: {resultado.modified_count}")
            client.close()
        except Exception as e:
            print(f"Erro ao conectar ou inserir dados limpos no MongoDB: {e}")