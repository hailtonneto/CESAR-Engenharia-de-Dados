import traceback
from src.pipeline import pipeline_licitacoes_flow

if __name__ == "__main__":
    print("Iniciando Orquestração Prefect...")
    try:
        pipeline_licitacoes_flow(data_final="20260530")
        print("Fluxo finalizado com sucesso!")
        
    except Exception as erro:
        print(f"Erro na orquestração: {erro}")
        print(traceback.format_exc())