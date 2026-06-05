import pandas as pd


class TransformadorDados:
    """
    Responsável pela camada de transformação (limpeza/normalização) das
    contratações extraídas do PNCP.

    O PNCP devolve documentos com campos aninhados (dicionários) que são
    essenciais para o frontend/API — por exemplo ``orgaoEntidade``,
    ``unidadeOrgao``, ``amparoLegal`` e os links oficiais. O Pandas, ao
    construir um DataFrame, "achata" parte desse conteúdo e perdíamos esses
    objetos quando restringíamos a uma lista pequena de colunas.

    Para resolver isso a classe expõe DOIS pontos de saída:

    * :meth:`transformar` — retorna um ``DataFrame`` achatado (tabular),
      usado para a carga no MySQL (camada gold relacional).
    * :meth:`transformar_registros` — retorna ``list[dict]`` com TODOS os
      campos preservados (inclusive os dicionários aninhados), usado para a
      carga na coleção ``contratacoes_limpas`` do MongoDB.

    Ambos compartilham a mesma higienização (filtro por valor, normalização
    de ``municipioNome``/``codigoIbge``), garantindo consistência entre as
    duas cargas.
    """

    # Campos de topo (escalares) preservados na saída tabular (MySQL) e
    # também garantidos nos registros do MongoDB.
    COLUNAS_ESCALARES = [
        "numeroCompra",
        "anoCompra",
        "objetoCompra",
        "valorTotalEstimado",
        "modalidadeNome",
        "modoDisputaNome",
        "situacaoCompraNome",
        "informacaoComplementar",
        "dataAberturaProposta",
        "dataEncerramentoProposta",
        "dataPublicacaoPncp",
        "dataAtualizacao",
        "dataAtualizacaoGlobal",
        "linkSistemaOrigem",
        "linkProcessoEletronico",
        "uf",
        "municipioNome",
        "codigoIbge",
    ]

    # Campos aninhados (dicionários / listas) que precisam ser preservados
    # nos registros enviados ao MongoDB.
    COLUNAS_ANINHADAS = [
        "orgaoEntidade",
        "unidadeOrgao",
        "amparoLegal",
        "linksOficiais",
    ]

    def _higienizar(
        self,
        dados_brutos: list,
        valor_maximo: float | None = None,
        uf: str = "pe",
    ) -> list[dict]:
        """
        Aplica filtro por valor e normaliza os campos derivados de
        ``unidadeOrgao`` (``municipioNome``/``codigoIbge``) e ``uf``,
        preservando integralmente cada documento (inclusive os dicionários
        aninhados).

        Retorna a lista de dicts higienizada.
        """
        dados_filtrados: list[dict] = []

        for item in dados_brutos:
            if not isinstance(item, dict):
                continue

            valor = item.get("valorTotalEstimado")
            try:
                if valor is None:
                    continue
                if valor_maximo is not None and float(valor) >= float(valor_maximo):
                    continue
            except (ValueError, TypeError):
                continue

            unidade = item.get("unidadeOrgao") or {}

            # Normaliza campos de localização "achatados" usados pela API,
            # mas mantém o dicionário ``unidadeOrgao`` original intacto.
            item["municipioNome"] = unidade.get("municipioNome", "Não Informado")
            item["codigoIbge"] = unidade.get("codigoIbge", "")

            # Garante o campo ``uf`` no topo do documento (a API filtra por
            # ele). Preferimos o que já vier do documento; senão usamos o
            # da unidade ou o parâmetro recebido.
            if not item.get("uf"):
                item["uf"] = unidade.get("ufSigla") or (uf.upper() if uf else "")

            # Unifica a grafia do link PNCP (a API aceita ``linkPncp``).
            if "linkPncp" not in item and item.get("linkPNCP") is not None:
                item["linkPncp"] = item.get("linkPNCP")

            dados_filtrados.append(item)

        return dados_filtrados

    def transformar_registros(
        self,
        dados_brutos: list,
        valor_maximo: float | None = None,
        uf: str = "pe",
    ) -> list[dict]:
        """
        Retorna a lista de registros higienizados com TODOS os campos
        preservados (escalares + dicionários aninhados como
        ``orgaoEntidade``, ``unidadeOrgao``, ``amparoLegal`` e os links
        oficiais).

        Destinado à carga no MongoDB (coleção ``contratacoes_limpas``), de
        onde o backend/API consome os objetos aninhados.
        """
        return self._higienizar(dados_brutos, valor_maximo=valor_maximo, uf=uf)

    def transformar(
        self,
        dados_brutos: list,
        valor_maximo: float | None = None,
        uf: str = "pe",
    ) -> pd.DataFrame:
        """
        Retorna um ``DataFrame`` achatado para a carga relacional no MySQL.

        Reaproveita a mesma higienização de :meth:`transformar_registros` e,
        a partir dos registros preservados, seleciona as colunas escalares
        de interesse (os dicionários aninhados não fazem parte do schema
        relacional). Colunas ausentes são simplesmente ignoradas.
        """
        registros = self._higienizar(dados_brutos, valor_maximo=valor_maximo, uf=uf)

        df = pd.DataFrame(registros)
        if df.empty:
            return df

        colunas_presentes = [c for c in self.COLUNAS_ESCALARES if c in df.columns]
        return df[colunas_presentes]
