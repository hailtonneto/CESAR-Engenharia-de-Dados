from prefect import flow, task
from src.extraction import ExtratorPNCP
from src.transformation import TransformadorDados
from src.database import DatabaseConnector

@task(retries=3, retry_delay_seconds=60, name="Extrair Dados PNCP")
def extrair_task(extrator, data_final):
    return extrator.extrair(data_final)

@task(name="Transformar Dados")
def transformar_task(transformador, dados_brutos):
    return transformador.transformar(dados_brutos)

@task(name="Carregar MySQL (Batch)")
def carregar_mysql_task(db_connector, df):
    db_connector.carregar_mysql(df)

# Vou implementar pós adicionar o Data Streaming na main, mas deixo o código comentado para referência futura.

#@task(name="Carregar MongoDB (Streaming)")
#def carregar_mongo_task(db_connector, dados):
#    db_connector.carregar_mongo(dados)

@flow(name="Pipeline ETL Licitações MEI")
def pipeline_licitacoes_flow(data_final: str = "20260530"):
    extrator = ExtratorPNCP()
    transformador = TransformadorDados()
    db_connector = DatabaseConnector()

    dados_brutos = extrair_task(extrator, data_final)
    
    if not dados_brutos:
        print("Aviso: Sem dados para processar.")
        return

    df_transformado = transformar_task(transformador, dados_brutos)
    
    if df_transformado.empty:
        print("Aviso: DataFrame vazio após transformação.")
        return

    carregar_mysql_task(db_connector, df_transformado)
    
    # Para futuro streaming

    # dados_streaming = df_transformado.to_dict(orient="records")
    # carregar_mongo_task(db_connector, dados_streaming)

if __name__ == "__main__":
    pipeline_licitacoes_flow()