import traceback

# Entrypoint canônico do pipeline: importa o FLUXO COMPLETO definido em
# `orchestrate_prefect.py` (extrai do PNCP, grava brutos no MongoDB,
# transforma preservando os campos aninhados e popula tanto a camada limpa
# `contratacoes_limpas` no MongoDB quanto a tabela relacional no MySQL).
#
# Observação: `src/pipeline.py` é LEGADO — contém apenas a versão reduzida do
# flow (extração + carga de brutos). O entrypoint oficial é este, que chama o
# flow completo de `orchestrate_prefect.py`.
from orchestrate_prefect import etl_pncp_flow

if __name__ == "__main__":
    print("Iniciando Orquestração Prefect (fluxo completo)...")
    try:
        # Sem datas fixas: a janela é móvel (últimos ETL_JANELA_DIAS dias até
        # hoje, default 30), calculada em runtime dentro do flow.
        etl_pncp_flow(uf="pe")
        print("Fluxo finalizado com sucesso!")

    except Exception as erro:
        print(f"Erro na orquestração: {erro}")
        print(traceback.format_exc())
