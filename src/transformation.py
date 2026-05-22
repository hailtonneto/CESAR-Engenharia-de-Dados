import pandas as pd

class TransformadorDados:
    def transformar(self, dados_brutos: list, valor_maximo: float | None = None, uf: str = "pe"):
        dados_filtrados = []
        
        for item in dados_brutos:
            valor = item.get('valorTotalEstimado')
            try:
                if valor is not None and (valor_maximo is None or float(valor) < valor_maximo):
                    unidade = item.get('unidadeOrgao', {})
                    item['municipioNome'] = unidade.get('municipioNome', 'Não Informado')
                    item['codigoIbge'] = unidade.get('codigoIbge', '')
                    dados_filtrados.append(item)
            except (ValueError, TypeError):
                continue
                
        df = pd.DataFrame(dados_filtrados)
        
        if df.empty:
            return df
            
        colunas_interesse = [
            'numeroCompra', 
            'anoCompra', 
            'objetoCompra', 
            'valorTotalEstimado', 
            'dataAtualizacao',
            'uf',
            'municipioNome'
        ]
        
        colunas_presentes = [coluna for coluna in colunas_interesse if coluna in df.columns]
        df_limpo = df[colunas_presentes]
        
        return df_limpo