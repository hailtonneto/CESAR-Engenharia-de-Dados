import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

class DatabaseConnector:
    def __init__(self):
        self.mysql_uri = os.getenv("MYSQL_URI")

    def carregar_mysql(self, df: pd.DataFrame, nome_tabela: str = "editais_recife"):
        if not df.empty:
            engine = create_engine(self.mysql_uri)
            df.to_sql(nome_tabela, con=engine, if_exists='replace', index=False)