import time
import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger("etl.extraction")


BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/proposta"
TAMANHO_PAGINA = 49 
MAX_TENTATIVAS = 3
TIMEOUT_SEGUNDOS = 15
PAUSA_ENTRE_PAGINAS = 0.5  


def _criar_sessao_com_retry() -> requests.Session:
    
    sessao = requests.Session()
    retry_policy = Retry(
        total=MAX_TENTATIVAS,
        backoff_factor=1,           
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_policy)
    sessao.mount("https://", adapter)
    sessao.mount("http://", adapter)
    return sessao


class ExtratorPNCP:
    

    def __init__(self):
        self.sessao = _criar_sessao_com_retry()

    def extrair(self, data_final: str, uf: str = "pe") -> list[dict]:
        
        todos_os_dados: list[dict] = []
        pagina = 1

        logger.info(f"Iniciando extração | UF={uf.upper()} | data_final={data_final}")

        while True:
            parametros = {
                "dataFinal": data_final,
                "uf": uf,
                "pagina": str(pagina),
                "tamanhoPagina": str(TAMANHO_PAGINA),
                
            }

            logger.info(f"Buscando página {pagina}...")

            try:
                resposta = self.sessao.get(
                    BASE_URL,
                    params=parametros,
                    timeout=TIMEOUT_SEGUNDOS,
                )
                resposta.raise_for_status()
                dados = resposta.json()

                if isinstance(dados, dict):
                    lista_pagina = dados.get("data") or dados.get("content") or []
                elif isinstance(dados, list):
                    lista_pagina = dados
                else:
                    lista_pagina = []

                if not lista_pagina:
                    logger.info(f"Página {pagina} vazia — extração concluída.")
                    break

                todos_os_dados.extend(lista_pagina)
                logger.info(f"Página {pagina} | +{len(lista_pagina)} registros | Total: {len(todos_os_dados)}")

                pagina += 1
                time.sleep(PAUSA_ENTRE_PAGINAS)

            except requests.exceptions.Timeout:
                logger.error(f"Timeout na página {pagina} após {TIMEOUT_SEGUNDOS}s. Abortando extração.")
                break

            except requests.exceptions.HTTPError as e:
                logger.error(f"Erro HTTP {e.response.status_code} na página {pagina}: {e}. Abortando.")
                break

            except requests.exceptions.RequestException as e:
                logger.error(f"Erro de rede na página {pagina}: {e}. Abortando.")
                break

        logger.info(f"Extração finalizada. Total extraído: {len(todos_os_dados)} registros.")
        return todos_os_dados