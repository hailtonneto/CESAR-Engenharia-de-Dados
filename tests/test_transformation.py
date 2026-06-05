"""
Testes da transformação/higienização (lógica PURA, sem rede/DB).

Cobre `TransformadorDados._higienizar` / `transformar_registros`, garantindo:
- preservação dos campos aninhados (orgaoEntidade, unidadeOrgao, amparoLegal);
- normalização de municipioNome/codigoIbge a partir de unidadeOrgao;
- garantia do campo uf no topo do documento;
- filtro por valor (ausente e por limite máximo);
- unificação da grafia do link PNCP.

Caso pandas não esteja disponível, os testes são pulados com motivo claro
(o módulo src.transformation importa pandas no topo).
"""
import pytest

transformation = pytest.importorskip(
    "src.transformation",
    reason="src.transformation requer pandas instalado.",
)
TransformadorDados = transformation.TransformadorDados


@pytest.fixture
def transformador():
    return TransformadorDados()


@pytest.fixture
def registro_completo():
    return {
        "numeroControlePNCP": "00000000000191-1-000123/2026",
        "numeroCompra": "123",
        "anoCompra": 2026,
        "objetoCompra": "Aquisição de material",
        "valorTotalEstimado": 50000,
        "orgaoEntidade": {"cnpj": "11222333000144", "razaoSocial": "Prefeitura"},
        "unidadeOrgao": {
            "municipioNome": "Recife",
            "codigoIbge": "2611606",
            "ufSigla": "PE",
        },
        "amparoLegal": {"nome": "Lei 14.133"},
        "linksOficiais": ["https://pncp.gov.br/x"],
        "linkPNCP": "https://pncp.gov.br/edital/123",
    }


def test_preserva_campos_aninhados(transformador, registro_completo):
    saida = transformador.transformar_registros([registro_completo], uf="pe")
    assert len(saida) == 1
    reg = saida[0]
    # Os dicionários/listas aninhados não podem ser descartados.
    assert reg["orgaoEntidade"] == {"cnpj": "11222333000144", "razaoSocial": "Prefeitura"}
    assert reg["unidadeOrgao"]["municipioNome"] == "Recife"
    assert reg["amparoLegal"] == {"nome": "Lei 14.133"}
    assert reg["linksOficiais"] == ["https://pncp.gov.br/x"]


def test_normaliza_municipio_e_codigo_ibge(transformador, registro_completo):
    reg = transformador.transformar_registros([registro_completo], uf="pe")[0]
    assert reg["municipioNome"] == "Recife"
    assert reg["codigoIbge"] == "2611606"


def test_municipio_default_quando_ausente(transformador):
    item = {"valorTotalEstimado": 100, "unidadeOrgao": {}}
    reg = transformador.transformar_registros([item], uf="pe")[0]
    assert reg["municipioNome"] == "Não Informado"
    assert reg["codigoIbge"] == ""


def test_garante_uf_no_topo_a_partir_da_unidade(transformador, registro_completo):
    reg = transformador.transformar_registros([registro_completo], uf="ba")[0]
    # ufSigla da unidade tem precedência sobre o parâmetro.
    assert reg["uf"] == "PE"


def test_garante_uf_no_topo_a_partir_do_parametro(transformador):
    item = {"valorTotalEstimado": 100, "unidadeOrgao": {}}
    reg = transformador.transformar_registros([item], uf="pe")[0]
    assert reg["uf"] == "PE"


def test_unifica_link_pncp(transformador, registro_completo):
    reg = transformador.transformar_registros([registro_completo], uf="pe")[0]
    assert reg["linkPncp"] == "https://pncp.gov.br/edital/123"


def test_filtra_registro_sem_valor(transformador):
    itens = [
        {"numeroCompra": "1", "valorTotalEstimado": None, "unidadeOrgao": {}},
        {"numeroCompra": "2", "valorTotalEstimado": 500, "unidadeOrgao": {}},
    ]
    saida = transformador.transformar_registros(itens, uf="pe")
    assert [r["numeroCompra"] for r in saida] == ["2"]


def test_filtra_por_valor_maximo(transformador):
    itens = [
        {"numeroCompra": "1", "valorTotalEstimado": 5000, "unidadeOrgao": {}},
        {"numeroCompra": "2", "valorTotalEstimado": 20000, "unidadeOrgao": {}},
    ]
    saida = transformador.transformar_registros(itens, valor_maximo=10000, uf="pe")
    # 20000 >= 10000 deve ser descartado; 5000 mantido.
    assert [r["numeroCompra"] for r in saida] == ["1"]


def test_ignora_itens_nao_dict(transformador):
    itens = ["string", 42, None, {"valorTotalEstimado": 1, "unidadeOrgao": {}}]
    saida = transformador.transformar_registros(itens, uf="pe")
    assert len(saida) == 1


def test_valor_invalido_descartado_quando_ha_limite(transformador):
    # Com valor_maximo definido, valores não numéricos quebram o float() e
    # o registro é descartado.
    itens = [{"numeroCompra": "1", "valorTotalEstimado": "abc", "unidadeOrgao": {}}]
    saida = transformador.transformar_registros(itens, valor_maximo=10000, uf="pe")
    assert saida == []


def test_valor_invalido_passa_sem_limite(transformador):
    # Sem valor_maximo não há conversão para float, então o registro (com
    # valor presente, ainda que não numérico) é mantido. Documenta o
    # comportamento atual de _higienizar.
    itens = [{"numeroCompra": "1", "valorTotalEstimado": "abc", "unidadeOrgao": {}}]
    saida = transformador.transformar_registros(itens, uf="pe")
    assert len(saida) == 1


def test_transformar_dataframe_apenas_escalares(transformador, registro_completo):
    df = transformador.transformar([registro_completo], uf="pe")
    # As colunas aninhadas não aparecem no DataFrame relacional.
    for aninhada in TransformadorDados.COLUNAS_ANINHADAS:
        assert aninhada not in df.columns
    assert "numeroCompra" in df.columns
    assert "municipioNome" in df.columns
