import traceback
from src.pipeline import etl_pncp_flow

if __name__ == "__main__":
    print("Iniciando Orquestração Prefect...")
    try:
        etl_pncp_flow(
            data_inicial="20260101",
            data_final="20260530",
            uf="pe",
        )
        print("Fluxo finalizado com sucesso!")
        
    except Exception as erro:
        print(f"Erro na orquestração: {erro}")
        print(traceback.format_exc())