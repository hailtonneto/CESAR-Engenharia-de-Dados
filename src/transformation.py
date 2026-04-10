import pandas as pd

class TransformadorDados:
    def transformar(self, dados_brutos: list):
        dados_filtrados = []
        
        for item in dados_brutos:
            valor = item.get('valorTotalEstimado')
            if valor is not None and float(valor) < 81000:
                dados_filtrados.append(item)
                
        df = pd.DataFrame(dados_filtrados)
        
        if df.empty:
            return df
            
        colunas_interesse = [
            'numeroCompra', 
            'anoCompra', 
            'objetoCompra', 
            'valorTotalEstimado', 
            'dataAtualizacao'
        ]
        
        colunas_presentes = [coluna for coluna in colunas_interesse if coluna in df.columns]
        df_limpo = df[colunas_presentes]
        
        return df_limpo