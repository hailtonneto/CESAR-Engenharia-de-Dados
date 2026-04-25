import logging
import pandas as pd

logger = logging.getLogger("etl.transformation")

COLUNAS_INTERESSE = [
    "numeroCompra",
    "anoCompra",
    "objetoCompra",
    "valorTotalEstimado",
    "dataAtualizacao",
    "uf",
    "codigoModalidadeContratacao",
    "nomeUnidadeOrgao",
    "municipioNome",
]


class TransformadorDados:

    def transformar(
        self,
        dados_brutos: list[dict],
        valor_maximo: float | None = None,
        uf: str | None = None,
    ) -> pd.DataFrame:
        
        if not dados_brutos:
            logger.warning("Nenhum dado bruto recebido para transformar.")
            return pd.DataFrame()

        logger.info(f"Iniciando transformação de {len(dados_brutos)} registros.")
        df = pd.DataFrame(dados_brutos)

        colunas_presentes = [c for c in COLUNAS_INTERESSE if c in df.columns]
        df = df[colunas_presentes].copy()
        logger.info(f"Colunas selecionadas: {colunas_presentes}")

        if "valorTotalEstimado" in df.columns:
            df["valorTotalEstimado"] = pd.to_numeric(
                df["valorTotalEstimado"], errors="coerce"
            )

        if "dataAtualizacao" in df.columns:
            df["dataAtualizacao"] = pd.to_datetime(
                df["dataAtualizacao"], errors="coerce"
            )

        antes = len(df)
        df = df.dropna(subset=["valorTotalEstimado"])
        logger.info(f"Removidos {antes - len(df)} registros sem valor estimado.")

        if valor_maximo is not None:
            antes = len(df)
            df = df[df["valorTotalEstimado"] <= valor_maximo]
            logger.info(
                f"Filtro valor_maximo={valor_maximo}: {antes - len(df)} registros removidos."
            )

        if uf is not None:
            uf_upper = uf.upper()
            if "uf" in df.columns:
                antes = len(df)
                df = df[df["uf"].str.upper() == uf_upper]
                logger.info(
                    f"Filtro UF={uf_upper}: {antes - len(df)} registros removidos."
                )

        logger.info(f"Transformação concluída. {len(df)} registros prontos para carga.")
        return df.reset_index(drop=True)