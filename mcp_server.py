import os
import json
from datetime import datetime

import requests
from dotenv import load_dotenv
from pymongo import MongoClient
from fastmcp import FastMCP

load_dotenv()

mcp = FastMCP(
    name="pncp-recife",
    instructions=(
        "Você é um assistente especializado em contratações públicas do PNCP "
        "(Portal Nacional de Contratações Públicas) com foco na cidade de Recife, PE. "
        "Use as ferramentas disponíveis para buscar dados reais do banco de dados "
        "e da API do PNCP. Responda sempre em português brasileiro de forma clara e objetiva."
    ),
)

MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "ETL")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "contratacoes_brutas")
PNCP_BASE_URL = "https://pncp.gov.br/api/consulta/v1/contratacoes/proposta"


def _get_mongo_collection():
    client = MongoClient(MONGO_URI, tlsAllowInvalidCertificates=True, serverSelectionTimeoutMS=8000)
    return client[MONGO_DB_NAME][MONGO_COLLECTION], client


@mcp.tool()
def buscar_contratacoes_pncp(
    data_inicial: str = "20250101",
    data_final: str = "20251231",
    uf: str = "pe",
    paginas: int = 2,
) -> str:
    """
    Busca contratações públicas diretamente na API do PNCP.

    Args:
        data_inicial: Data de início no formato YYYYMMDD (ex: '20250101').
        data_final:   Data de fim no formato YYYYMMDD (ex: '20251231').
        uf:           Sigla do estado em minúsculas (ex: 'pe').
        paginas:      Número de páginas a buscar (máx 5).
    """
    paginas = min(max(int(paginas), 1), 5)
    todos = []

    for pagina in range(1, paginas + 1):
        params = {
            "dataInicial": data_inicial,
            "dataFinal": data_final,
            "codigoModalidadeContratacao": "8",
            "uf": uf.lower(),
            "codigoMunicipiolbge": "2611606",
            "pagina": str(pagina),
            "tamanhoPagina": "20",
        }
        try:
            resp = requests.get(PNCP_BASE_URL, params=params, timeout=15)
            resp.raise_for_status()
            dados = resp.json()
            lista = dados.get("data", dados) if isinstance(dados, dict) else dados
            if not lista:
                break
            todos.extend(lista if isinstance(lista, list) else [])
        except Exception as e:
            return json.dumps({"erro": f"Falha na página {pagina}: {str(e)}"}, ensure_ascii=False)

    resumo = []
    for item in todos:
        resumo.append({
            "numeroCompra": item.get("numeroCompra"),
            "anoCompra": item.get("anoCompra"),
            "objetoCompra": item.get("objetoCompra", "")[:200],
            "valorTotalEstimado": item.get("valorTotalEstimado"),
            "dataAtualizacao": item.get("dataAtualizacao"),
            "municipio": item.get("unidadeOrgao", {}).get("municipioNome"),
            "orgao": item.get("unidadeOrgao", {}).get("nomeUnidade"),
        })

    return json.dumps({"total": len(resumo), "contratacoes": resumo}, ensure_ascii=False, default=str)


@mcp.tool()
def consultar_mongodb(filtro_json: str = "{}", limite: int = 10) -> str:
    """
    Consulta contratações armazenadas no MongoDB Atlas.

    Args:
        filtro_json: Filtro MongoDB em formato JSON. Use '{}' para buscar todos.
        limite:      Número máximo de documentos a retornar (máx 50).
    """
    limite = min(max(int(limite), 1), 50)

    try:
        filtro = json.loads(filtro_json)
    except json.JSONDecodeError:
        return json.dumps({"erro": "filtro_json inválido — use formato JSON válido."}, ensure_ascii=False)

    try:
        col, client = _get_mongo_collection()
        docs = list(col.find(filtro, {"_id": 0}).limit(limite))
        client.close()

        simplificados = []
        for d in docs:
            simplificados.append({
                "numeroCompra": d.get("numeroCompra"),
                "anoCompra": d.get("anoCompra"),
                "objetoCompra": str(d.get("objetoCompra", ""))[:200],
                "valorTotalEstimado": d.get("valorTotalEstimado"),
                "dataAtualizacao": d.get("dataAtualizacao"),
                "uf": d.get("uf"),
                "municipioNome": d.get("municipioNome"),
                "run_id": d.get("run_id"),
            })

        return json.dumps({"total_retornado": len(simplificados), "documentos": simplificados}, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"erro": str(e)}, ensure_ascii=False)


@mcp.tool()
def resumir_contratacoes_mongodb() -> str:
    """Retorna estatísticas agregadas das contratações no MongoDB."""
    try:
        col, client = _get_mongo_collection()

        total = col.count_documents({})

        pipeline_valor = [
            {"$match": {"valorTotalEstimado": {"$type": "double"}}},
            {"$group": {
                "_id": None,
                "soma": {"$sum": "$valorTotalEstimado"},
                "media": {"$avg": "$valorTotalEstimado"},
                "max": {"$max": "$valorTotalEstimado"},
                "min": {"$min": "$valorTotalEstimado"},
            }},
        ]
        valores = list(col.aggregate(pipeline_valor))
        stats_valores = valores[0] if valores else {}
        stats_valores.pop("_id", None)

        municipios = [
            {"municipio": r["_id"], "total": r["count"]}
            for r in col.aggregate([
                {"$group": {"_id": "$municipioNome", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5},
            ])
        ]

        anos = [
            {"ano": r["_id"], "total": r["count"]}
            for r in col.aggregate([
                {"$group": {"_id": "$anoCompra", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": 5},
            ])
        ]

        client.close()

        return json.dumps({
            "total_registros": total,
            "estatisticas_valor": {k: round(v, 2) for k, v in stats_valores.items() if isinstance(v, (int, float))},
            "top_municipios": municipios,
            "top_anos": anos,
        }, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"erro": str(e)}, ensure_ascii=False)


@mcp.tool()
def buscar_contratacao_por_numero(numero_compra: str) -> str:
    """
    Busca uma contratação específica pelo número de compra no MongoDB.

    Args:
        numero_compra: Número da compra (ex: '90001/2025').
    """
    try:
        col, client = _get_mongo_collection()
        doc = col.find_one({"numeroCompra": numero_compra}, {"_id": 0})
        client.close()

        if not doc:
            return json.dumps({"encontrado": False, "mensagem": f"Contratação '{numero_compra}' não encontrada."}, ensure_ascii=False)

        doc.pop("unidadeOrgao", None)
        return json.dumps({"encontrado": True, "dados": doc}, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"erro": str(e)}, ensure_ascii=False)


@mcp.tool()
def listar_municipios_disponiveis() -> str:
    """Lista todos os municípios com contratações no banco, com contagem de registros."""
    try:
        col, client = _get_mongo_collection()
        resultado = [
            {"municipio": r["_id"] or "Não informado", "total": r["total"]}
            for r in col.aggregate([
                {"$group": {"_id": "$municipioNome", "total": {"$sum": 1}}},
                {"$sort": {"total": -1}},
            ])
        ]
        client.close()
        return json.dumps({"municipios": resultado, "total_municipios": len(resultado)}, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"erro": str(e)}, ensure_ascii=False)


@mcp.tool()
def status_pipeline() -> str:
    """Retorna o status atual do pipeline ETL e metadados do banco de dados."""
    try:
        col, client = _get_mongo_collection()
        total = col.count_documents({})
        ultimo = col.find_one(
            {"run_id": {"$exists": True}},
            {"_id": 0, "run_id": 1, "dataAtualizacao": 1},
            sort=[("dataAtualizacao", -1)],
        )
        client.close()

        return json.dumps({
            "status": "online",
            "banco_de_dados": MONGO_DB_NAME,
            "colecao": MONGO_COLLECTION,
            "total_registros": total,
            "ultimo_run_id": ultimo.get("run_id") if ultimo else "N/A",
            "ultima_atualizacao": ultimo.get("dataAtualizacao") if ultimo else "N/A",
            "verificado_em": datetime.now().isoformat(),
        }, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"status": "erro", "erro": str(e), "verificado_em": datetime.now().isoformat()}, ensure_ascii=False)


@mcp.tool()
def buscar_contratacoes_por_valor(valor_minimo: float = 0.0, valor_maximo: float = 10000000.0, limite: int = 10) -> str:
    """
    Busca contratações filtradas por faixa de valor estimado.

    Args:
        valor_minimo: Valor mínimo em reais (padrão: 0).
        valor_maximo: Valor máximo em reais (padrão: 10.000.000).
        limite:       Número máximo de registros a retornar (máx 50).
    """
    limite = min(max(int(limite), 1), 50)
    filtro = {"valorTotalEstimado": {"$gte": float(valor_minimo), "$lte": float(valor_maximo)}}

    try:
        col, client = _get_mongo_collection()
        docs = list(col.find(filtro, {"_id": 0}).sort("valorTotalEstimado", -1).limit(limite))
        client.close()

        resultado = [{
            "numeroCompra": d.get("numeroCompra"),
            "anoCompra": d.get("anoCompra"),
            "objetoCompra": str(d.get("objetoCompra", ""))[:200],
            "valorTotalEstimado": d.get("valorTotalEstimado"),
            "municipioNome": d.get("municipioNome"),
            "dataAtualizacao": d.get("dataAtualizacao"),
        } for d in docs]

        return json.dumps({
            "filtro": {"valor_minimo": valor_minimo, "valor_maximo": valor_maximo},
            "total_retornado": len(resultado),
            "contratacoes": resultado,
        }, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"erro": str(e)}, ensure_ascii=False)


@mcp.tool()
def buscar_contratacoes_por_objeto(termo: str, limite: int = 10) -> str:
    """
    Busca contratações cujo objeto contenha o termo especificado.

    Args:
        termo:  Palavra ou expressão a pesquisar (case-insensitive).
        limite: Número máximo de registros a retornar (máx 50).
    """
    limite = min(max(int(limite), 1), 50)
    filtro = {"objetoCompra": {"$regex": termo, "$options": "i"}}

    try:
        col, client = _get_mongo_collection()
        docs = list(col.find(filtro, {"_id": 0}).limit(limite))
        client.close()

        resultado = [{
            "numeroCompra": d.get("numeroCompra"),
            "anoCompra": d.get("anoCompra"),
            "objetoCompra": str(d.get("objetoCompra", ""))[:300],
            "valorTotalEstimado": d.get("valorTotalEstimado"),
            "municipioNome": d.get("municipioNome"),
            "dataAtualizacao": d.get("dataAtualizacao"),
        } for d in docs]

        return json.dumps({"termo_buscado": termo, "total_retornado": len(resultado), "contratacoes": resultado}, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"erro": str(e)}, ensure_ascii=False)


if __name__ == "__main__":
    mcp.run(transport="stdio")