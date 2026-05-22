import uuid
import pandas as pd

from prefect import flow, task, get_run_logger
from dotenv import load_dotenv

from src.extraction import ExtratorPNCP
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


@flow(
    name="ETL PNCP — Contratações Pernambuco",
    description=(
        "Pipeline completo: extrai contratações do PNCP e persiste dados brutos "
        "no MongoDB (bronze). Transformação e carga MySQL feitas pelo PySpark."
    ),
    log_prints=True,
)
def etl_pncp_flow(
    data_inicial: str = "20250101",
    data_final: str = "20250430",
    uf: str = "pe",
) -> None:
    run_id = str(uuid.uuid4())
    logger = get_run_logger()

    logger.info("=" * 60)
    logger.info(f"PIPELINE INICIADO  |  run_id={run_id}")
    logger.info(
        f"Parâmetros: uf={uf.upper()} | {data_inicial} → {data_final}"
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

    logger.info("=" * 60)
    logger.info(f"PIPELINE CONCLUÍDO COM SUCESSO  |  run_id={run_id}")
    logger.info("=" * 60)


if __name__ == "__main__":
    etl_pncp_flow(
        data_inicial="20260101",
        data_final="20260530",
        uf="pe",
    )