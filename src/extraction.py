import requests

class ExtratorPNCP:
    def __init__(self):
        self.base_url = "https://pncp.gov.br/api/consulta/v1/contratacoes/proposta"

    def extrair(self, data_inicial: str, data_final: str, uf: str):
        todos_os_dados = []
        paginas_a_extrair = 5
        
        for pagina in range(1, paginas_a_extrair + 1):
            parametros = {
                "dataInicial": data_inicial,
                "dataFinal": data_final,
                "codigoModalidadeContratacao": "8",
                "uf": uf,
                "codigoMunicipiolbge": "2611606",
                "pagina": str(pagina),
                "tamanhoPagina": "20"
            }
            
            try:
                resposta = requests.get(self.base_url, params=parametros, timeout=15)
                resposta.raise_for_status()
                
                dados = resposta.json()
                
                if isinstance(dados, dict) and "data" in dados:
                    lista_pagina = dados["data"]
                else:
                    lista_pagina = dados if isinstance(dados, list) else []
                    
                if not lista_pagina:
                    break
                    
                todos_os_dados.extend(lista_pagina)
                
            except requests.exceptions.RequestException as erro:
                print(f"Erro de conexão na página {pagina}: {erro}")
                raise erro
                
        return todos_os_dados