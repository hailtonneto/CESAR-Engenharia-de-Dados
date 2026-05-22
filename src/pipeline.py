import uuid
import logging
from src.extraction import ExtratorPNCP
from src.transformation import TransformadorDados
from src.database import DatabaseConnector

logger = logging.getLogger("etl.pipeline")


class PipelineETL:

    def __init__(self):
        self.extrator = ExtratorPNCP()
        self.transformador = TransformadorDados()
        self.db_connector = DatabaseConnector()

    def executar(
        self,
        data_final: str,
        uf: str = "pe",
        valor_maximo: float | None = None,
    ) -> None:
        
        run_id = str(uuid.uuid4())  
        logger.info(f"{'='*60}")
        logger.info(f"PIPELINE INICIADO | run_id={run_id}")
        logger.info(f"Parâmetros: uf={uf.upper()} | data_final={data_final} | valor_maximo={valor_maximo}")
        logger.info(f"{'='*60}")

        dados_brutos = self.extrator.extrair(data_final=data_final, uf=uf)
        logger.info(f"[EXTRAÇÃO] {len(dados_brutos)} registros brutos obtidos da API.")

        if not dados_brutos:
            logger.warning("[PIPELINE] API retornou vazio. Nenhum dado para processar.")
            return

        logger.info("[CARGA] Enviando dados brutos ao MongoDB Atlas...")
        self.db_connector.carregar_mongodb(dados_brutos, run_id=run_id)

        logger.info("[TRANSFORMAÇÃO] Iniciando limpeza e filtragem...")
        df_transformado = self.transformador.transformar(
            dados_brutos,
            valor_maximo=valor_maximo,
            uf=uf,
        )
        logger.info(f"[TRANSFORMAÇÃO] {len(df_transformado)} registros após transformação.")

        if df_transformado.empty:
            logger.warning("[PIPELINE] Nenhum dado sobrou após transformação. MySQL não será atualizado.")
            return

        logger.info("[CARGA] Enviando dados transformados ao MySQL...")
        self.db_connector.carregar_mysql(df_transformado)

        logger.info(f"{'='*60}")
        logger.info(f"PIPELINE CONCLUÍDO COM SUCESSO | run_id={run_id}")
        logger.info(f"{'='*60}")