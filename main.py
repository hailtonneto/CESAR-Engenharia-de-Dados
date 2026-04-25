
import logging
import traceback
 
 
def configurar_logging():

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
 
 
def obter_parametros_usuario() -> dict:
    
    print("\n" + "="*55)
    print("  PIPELINE ETL — Portal Nacional de Contratações (PNCP)")
    print("="*55)
 
    uf = input("\nInforme a UF (ex: pe, sp, rj) [padrão: pe]: ").strip().lower()
    if not uf:
        uf = "pe"
 
    data_final = input("Data final da busca (YYYYMMDD) [padrão: 20260430]: ").strip()
    if not data_final:
        data_final = "20260430"
 
    valor_str = input("Valor máximo dos contratos (ex: 81000) [Enter para sem limite]: ").strip()
    valor_maximo = float(valor_str) if valor_str else None
 
    print("\n" + "-"*55)
    print(f"  UF selecionada : {uf.upper()}")
    print(f"  Data final     : {data_final}")
    print(f"  Valor máximo   : {'Sem limite' if valor_maximo is None else f'R$ {valor_maximo:,.2f}'}")
    print("-"*55 + "\n")
 
    return {"uf": uf, "data_final": data_final, "valor_maximo": valor_maximo}
 
 
if __name__ == "__main__":
    configurar_logging()
    logger = logging.getLogger("etl.main")
 
    try:
        parametros = obter_parametros_usuario()
 
        from src.pipeline import PipelineETL
        logger.info("Módulos carregados. Iniciando pipeline...")
 
        etl = PipelineETL()
        etl.executar(
            data_final=parametros["data_final"],
            uf=parametros["uf"],
            valor_maximo=parametros["valor_maximo"],
        )
 
    except KeyboardInterrupt:
        print("\n\nExecução cancelada pelo usuário.")
 
    except Exception:
        logger.critical("Erro fatal na execução do pipeline:")
        logger.critical(traceback.format_exc())