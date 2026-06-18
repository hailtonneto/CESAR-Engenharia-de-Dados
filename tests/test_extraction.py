from src.extraction import ExtratorPNCP


class RespostaFake:
    def __init__(self, dados):
        self._dados = dados

    def raise_for_status(self):
        return None

    def json(self):
        return self._dados


class RespostaJsonInvalido:
    def raise_for_status(self):
        return None

    def json(self):
        raise ValueError("JSON invalido")


def test_extracao_usa_50_por_padrao(monkeypatch):
    chamadas = []

    def get_fake(url, params, timeout):
        chamadas.append((url, params))
        return RespostaFake({"data": []})

    monkeypatch.setattr("src.extraction.requests.get", get_fake)

    ExtratorPNCP().extrair("20260101", "20260131", "pe")

    assert chamadas[0][0].endswith("/contratacoes/publicacao")
    assert chamadas[0][1]["tamanhoPagina"] == "50"
    assert chamadas[0][1]["codigoMunicipioIbge"] == "2611606"
    assert "codigoMunicipiolbge" not in chamadas[0][1]


def test_extracao_limita_tamanho_pagina_em_50(monkeypatch):
    chamadas = []

    def get_fake(url, params, timeout):
        chamadas.append(params)
        return RespostaFake({"data": []})

    monkeypatch.setattr("src.extraction.requests.get", get_fake)

    ExtratorPNCP().extrair("20260101", "20260131", "pe", tamanho_pagina=200)

    assert chamadas[0]["tamanhoPagina"] == "50"


def test_extracao_nao_aceita_tamanho_menor_que_1(monkeypatch):
    chamadas = []

    def get_fake(url, params, timeout):
        chamadas.append(params)
        return RespostaFake({"data": []})

    monkeypatch.setattr("src.extraction.requests.get", get_fake)

    ExtratorPNCP().extrair("20260101", "20260131", "pe", tamanho_pagina=0)

    assert chamadas[0]["tamanhoPagina"] == "1"


def test_extracao_tenta_novamente_quando_json_vem_invalido(monkeypatch):
    respostas = [
        RespostaJsonInvalido(),
        RespostaFake({"data": [{"numeroCompra": "1"}]}),
        RespostaFake({"data": []}),
    ]

    def get_fake(url, params, timeout):
        return respostas.pop(0)

    monkeypatch.setattr("src.extraction.requests.get", get_fake)

    dados = ExtratorPNCP().extrair(
        "20260101",
        "20260131",
        "pe",
        intervalo_tentativas=0,
    )

    assert dados == [{"numeroCompra": "1"}]
