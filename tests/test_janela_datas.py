"""
Testes da janela de datas MÓVEL (lógica PURA, sem rede/DB).

Cobre `janela_datas` de `orchestrate_prefect.py`: cálculo da janela relativa
(últimos N dias até amanhã) e leitura de ETL_JANELA_DIAS.

O módulo orchestrate_prefect importa prefect no topo; se prefect não estiver
instalado, os testes são pulados com motivo claro.
"""
from datetime import datetime, timedelta

import pytest

orch = pytest.importorskip(
    "orchestrate_prefect",
    reason="orchestrate_prefect requer prefect instalado.",
)
janela_datas = orch.janela_datas


def test_janela_explicita_30_dias():
    inicio, fim = janela_datas(30)
    d_ini = datetime.strptime(inicio, "%Y%m%d")
    d_fim = datetime.strptime(fim, "%Y%m%d")
    assert (d_fim - d_ini) == timedelta(days=31)


def test_fim_e_amanha():
    _, fim = janela_datas(7)
    assert fim == (datetime.now() + timedelta(days=1)).strftime("%Y%m%d")


def test_le_env(monkeypatch):
    monkeypatch.setenv("ETL_JANELA_DIAS", "10")
    inicio, fim = janela_datas()
    d_ini = datetime.strptime(inicio, "%Y%m%d")
    d_fim = datetime.strptime(fim, "%Y%m%d")
    assert (d_fim - d_ini) == timedelta(days=11)


def test_env_invalido_usa_default(monkeypatch):
    monkeypatch.setenv("ETL_JANELA_DIAS", "abc")
    inicio, fim = janela_datas()
    d_ini = datetime.strptime(inicio, "%Y%m%d")
    d_fim = datetime.strptime(fim, "%Y%m%d")
    assert (d_fim - d_ini) == timedelta(days=31)
