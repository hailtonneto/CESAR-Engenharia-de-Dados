"""
Agendamento (schedule) do pipeline ETL do PNCP.

Mantém o flow `etl_pncp_flow` (de `orchestrate_prefect.py`) servindo
continuamente e o executa de forma recorrente via cron diário às 06:00
(`0 6 * * *`). Compatível com Prefect 3.x (`flow.serve`).

Como ativar:

    pip install -r requirements.txt
    python serve_prefect.py

O processo fica em primeiro plano. Enquanto estiver rodando, o Prefect dispara
o pipeline todo dia às 06:00 usando a janela de datas MÓVEL (últimos
`ETL_JANELA_DIAS` dias até hoje — sem datas fixas).

O cron pode ser sobrescrito pela variável de ambiente `ETL_CRON`.

Alternativa (deployment declarativo): veja `prefect.yaml` e rode
`prefect deploy` + `prefect worker start`.
"""
import os

from dotenv import load_dotenv
from prefect.schedules import Cron

from orchestrate_prefect import etl_pncp_flow

load_dotenv()


if __name__ == "__main__":
    cron = os.getenv("ETL_CRON", "0 6 * * *")
    print(f"Servindo o flow ETL PNCP com agendamento cron='{cron}' (timezone America/Recife)...")

    # Não passamos data_inicial/data_final: o flow calcula a janela móvel em
    # runtime a cada execução agendada. O timezone vai no objeto Cron
    # (compatível com Prefect 3.x — flow.serve não aceita `timezone` direto).
    etl_pncp_flow.serve(
        name="etl-pncp-diario",
        schedules=[Cron(cron, timezone="America/Recife")],
        parameters={"uf": "pe"},
        description=(
            "Execução diária do pipeline ETL do PNCP com janela de datas "
            "móvel (ETL_JANELA_DIAS)."
        ),
    )
