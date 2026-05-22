import uuid
import pandas as pd

from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

from src.extraction import ExtratorPNCP
from src.transformation import TransformadorDados
from src.database import DatabaseConnector

load_dotenv()


@task(
    name="Extração PNCP",
    retries=3,
    retry_delay_seconds=15,
    description="Busca contratações na API do PNCP página a página.",
)
def task_extrair(data_inicial: str, data_final: str, uf: str) -> list[dict]:
    
    logger = get_run_logger()
    logger.info(
        f"Iniciando extração | UF={uf.upper()} "
        f"| data_inicial={data_inicial} | data_final={data_final}"
    )

    extrator = ExtratorPNCP()
    dados = extrator.extrair(
        data_inicial=data_inicial,
        data_final=data_final,
        uf=uf,
    )

    logger.info(f"{len(dados)} registros extraídos da API PNCP.")
    return dados


@task(
    name="Carga MongoDB (brutos)",
    retries=2,
    retry_delay_seconds=10,
    description="Persiste os dados brutos no MongoDB Atlas (camada raw/bronze).",
)
def task_carregar_mongodb(dados_brutos: list[dict], run_id: str) -> None:
    
    logger = get_run_logger()

    if not dados_brutos:
        logger.warning("Nenhum dado bruto para inserir no MongoDB.")
        return

    db = DatabaseConnector()
    db.carregar_mongodb(dados_brutos, run_id=run_id)
    logger.info(f"{len(dados_brutos)} documentos enviados ao MongoDB Atlas.")


@task(
    name="Transformação dos Dados",
    description="Limpa, filtra e normaliza os dados brutos.",
)
def task_transformar(
    dados_brutos: list[dict],
    valor_maximo: float | None,
    uf: str,
) -> list[dict]:
    
    logger = get_run_logger()
    logger.info(f"Iniciando transformação de {len(dados_brutos)} registros.")

    transformador = TransformadorDados()
    df = transformador.transformar(dados_brutos, valor_maximo=valor_maximo, uf=uf)

    logger.info(f"{len(df)} registros prontos após transformação.")
    return df.to_dict(orient="records")


@task(
    name="Carga MySQL (transformados)",
    retries=2,
    retry_delay_seconds=10,
    description="Insere os dados limpos na tabela MySQL (camada gold).",
)
def task_carregar_mysql(registros: list[dict], nome_tabela: str) -> None:
    
    logger = get_run_logger()

    if not registros:
        logger.warning("Nenhum registro transformado para inserir no MySQL.")
        return

    df = pd.DataFrame(registros)
    db = DatabaseConnector()
    db.carregar_mysql(df, nome_tabela=nome_tabela)
    logger.info(f"{len(df)} linhas inseridas na tabela '{nome_tabela}'.")


@flow(
    name="ETL PNCP — Contratações Pernambuco",
    description=(
        "Pipeline completo: extrai contratações do PNCP, persiste dados brutos "
        "no MongoDB (bronze) e dados transformados no MySQL (gold)."
    ),
    log_prints=True,
)
def etl_pncp_flow(
    data_inicial: str = "20250101",
    data_final: str = "20250430",
    uf: str = "pe",
    valor_maximo: float | None = None,
    nome_tabela_mysql: str = "editais_recife",
) -> None:
    
    run_id = str(uuid.uuid4())
    logger = get_run_logger()

    logger.info("=" * 60)
    logger.info(f"PIPELINE INICIADO  |  run_id={run_id}")
    logger.info(
        f"Parâmetros: uf={uf.upper()} | {data_inicial} → {data_final} "
        f"| valor_maximo={valor_maximo or 'Sem limite'}"
    )
    logger.info("=" * 60)

    dados_brutos = task_extrair(
        data_inicial=data_inicial,
        data_final=data_final,
        uf=uf,
    )

    if not dados_brutos:
        logger.warning("API retornou vazio. Pipeline encerrado sem carga.")
        return

    task_carregar_mongodb(dados_brutos=dados_brutos, run_id=run_id)

    registros_limpos = task_transformar(
        dados_brutos=dados_brutos,
        valor_maximo=valor_maximo,
        uf=uf,
    )

    task_carregar_mysql(
        registros=registros_limpos,
        nome_tabela=nome_tabela_mysql,
    )

    logger.info("=" * 60)
    logger.info(f"PIPELINE CONCLUÍDO COM SUCESSO  |  run_id={run_id}")
    logger.info("=" * 60)


if __name__ == "__main__":
    etl_pncp_flow(
        data_inicial="20260101",
        data_final="20260508",
        uf="pe",
    )