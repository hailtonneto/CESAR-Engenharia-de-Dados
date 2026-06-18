import time

import requests


class ExtratorPNCP:
    TAMANHO_MAXIMO_PAGINA = 50
    TENTATIVAS_POR_PAGINA = 3
    INTERVALO_TENTATIVAS = 2

    def __init__(self):
        self.base_url = "https://pncp.gov.br/api/consulta/v1/contratacoes/publicacao"

    def extrair(
        self,
        data_inicial: str,
        data_final: str,
        uf: str,
        tamanho_pagina: int = 50,
        tentativas_por_pagina: int = TENTATIVAS_POR_PAGINA,
        intervalo_tentativas: int = INTERVALO_TENTATIVAS,
    ):
        todos_os_dados = []
        paginas_a_extrair = 5
        tamanho_pagina = max(1, min(int(tamanho_pagina), self.TAMANHO_MAXIMO_PAGINA))

        for pagina in range(1, paginas_a_extrair + 1):
            parametros = {
                "dataInicial": data_inicial,
                "dataFinal": data_final,
                "codigoModalidadeContratacao": "8",
                "uf": uf,
                "codigoMunicipioIbge": "2611606",
                "pagina": str(pagina),
                "tamanhoPagina": str(tamanho_pagina),
            }

            dados = self._buscar_pagina(
                parametros=parametros,
                pagina=pagina,
                tentativas=tentativas_por_pagina,
                intervalo=intervalo_tentativas,
            )

            if isinstance(dados, dict) and "data" in dados:
                lista_pagina = dados["data"]
            else:
                lista_pagina = dados if isinstance(dados, list) else []

            if not lista_pagina:
                break

            todos_os_dados.extend(lista_pagina)

        return todos_os_dados

    def _buscar_pagina(self, parametros, pagina: int, tentativas: int, intervalo: int):
        for tentativa in range(1, tentativas + 1):
            try:
                resposta = requests.get(self.base_url, params=parametros, timeout=30)
                resposta.raise_for_status()
                return resposta.json()
            except ValueError as erro:
                if tentativa == tentativas:
                    print(f"Resposta invalida na pagina {pagina}: {erro}")
                    raise erro

                print(
                    f"Resposta invalida na pagina {pagina}. "
                    f"Tentando novamente ({tentativa}/{tentativas})..."
                )
            except requests.exceptions.RequestException as erro:
                if tentativa == tentativas:
                    print(f"Erro de conexao na pagina {pagina}: {erro}")
                    raise erro

                print(
                    f"Erro de conexao na pagina {pagina}: {erro}. "
                    f"Tentando novamente ({tentativa}/{tentativas})..."
                )

            time.sleep(intervalo)
