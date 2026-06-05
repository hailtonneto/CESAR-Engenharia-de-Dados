"""
Testes da chave de upsert composta (lógica PURA, sem rede/DB).

Cobre `montar_filtro_upsert` de `src/database.py`, garantindo:
- preferência por `numeroControlePNCP` quando presente;
- composição por `orgaoEntidade.cnpj` + `anoCompra` + `numeroCompra`;
- fallback para `numeroCompra` quando faltam CNPJ/ano.
"""
import pytest

# O módulo src.database importa pandas/pymongo/sqlalchemy/dotenv no topo.
# Caso alguma dependência falte no ambiente, pulamos com motivo claro
# (a função em si é pura e independe de rede/DB).
db = pytest.importorskip(
    "src.database",
    reason="src.database requer pandas/pymongo/sqlalchemy/dotenv instalados.",
)
montar_filtro_upsert = db.montar_filtro_upsert


def test_prefere_numero_controle_pncp():
    item = {
        "numeroControlePNCP": "00000000000191-1-000123/2026",
        "numeroCompra": "123",
        "anoCompra": 2026,
        "orgaoEntidade": {"cnpj": "00000000000191"},
    }
    assert montar_filtro_upsert(item) == {
        "numeroControlePNCP": "00000000000191-1-000123/2026"
    }


def test_chave_composta_cnpj_ano_numero():
    item = {
        "numeroCompra": "123",
        "anoCompra": 2026,
        "orgaoEntidade": {"cnpj": "11222333000144"},
    }
    assert montar_filtro_upsert(item) == {
        "orgaoEntidade.cnpj": "11222333000144",
        "anoCompra": 2026,
        "numeroCompra": "123",
    }


def test_chave_composta_distingue_mesmo_numero_compra():
    """numeroCompra repetido em órgãos diferentes deve gerar chaves distintas."""
    base = {"numeroCompra": "1", "anoCompra": 2026}
    a = montar_filtro_upsert({**base, "orgaoEntidade": {"cnpj": "111"}})
    b = montar_filtro_upsert({**base, "orgaoEntidade": {"cnpj": "222"}})
    assert a != b
    assert a["orgaoEntidade.cnpj"] == "111"
    assert b["orgaoEntidade.cnpj"] == "222"


def test_fallback_sem_cnpj_e_sem_ano():
    item = {"numeroCompra": "999"}
    assert montar_filtro_upsert(item) == {"numeroCompra": "999"}


def test_fallback_sem_cnpj_com_ano():
    item = {"numeroCompra": "999", "anoCompra": 2026}
    # Sem CNPJ não há como compor a chave estável: recai em numeroCompra.
    assert montar_filtro_upsert(item) == {"numeroCompra": "999"}


def test_orgao_entidade_none_nao_quebra():
    item = {"numeroCompra": "5", "anoCompra": 2026, "orgaoEntidade": None}
    assert montar_filtro_upsert(item) == {"numeroCompra": "5"}


def test_numero_controle_vazio_cai_para_composta():
    item = {
        "numeroControlePNCP": "",
        "numeroCompra": "7",
        "anoCompra": 2026,
        "orgaoEntidade": {"cnpj": "333"},
    }
    assert montar_filtro_upsert(item) == {
        "orgaoEntidade.cnpj": "333",
        "anoCompra": 2026,
        "numeroCompra": "7",
    }
