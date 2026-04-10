import traceback

if __name__ == "__main__":
    print("1. Iniciando o programa ETL...")
    try:
        from src.pipeline import PipelineETL
        print("2. Bibliotecas carregadas com sucesso!")
        
        etl = PipelineETL()
        print("3. Conectando à API de Consultas...")
        
        etl.executar("20260430")
        
        print("4. Processo finalizado. Verifique seu banco de dados!")
        
    except Exception as erro:
        print("Ocorreu um erro na execução do código:")
        print(traceback.format_exc())