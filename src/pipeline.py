from src.extraction import ExtratorPNCP
from src.transformation import TransformadorDados
from src.database import DatabaseConnector

class PipelineETL:
    def __init__(self):
        self.extrator = ExtratorPNCP()
        self.transformador = TransformadorDados()
        self.db_connector = DatabaseConnector()

    def executar(self, data_final: str):
        print(f"Buscando propostas em aberto no PE até: {data_final}...")
        
        dados_brutos = self.extrator.extrair(data_final)
        print(f"Encontrados na API: {len(dados_brutos)} registros!")
        
        if not dados_brutos:
            print("Aviso: A API retornou vazio! Nada para salvar no banco.")
            return
            
        df_transformado = self.transformador.transformar(dados_brutos)
        print(f"Dados transformados com sucesso! Linhas prontas: {len(df_transformado)}")
        
        self.db_connector.carregar_mysql(df_transformado)
        print("Tabela editais_recife criada e dados salvos no MySQL!")

        dados_streaming = df_transformado.to_dict(orient="records")
        self.db_connector.carregar_mongo(dados_streaming)
        print("Dados salvos no MongoDB! Pronto para streaming e notificações!")