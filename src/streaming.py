import os
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

load_dotenv()

client = AsyncIOMotorClient(os.getenv("MONGO_URI"), tlsAllowInvalidCertificates=True)
db = client[os.getenv("MONGO_DB_NAME")]
collection = db[os.getenv("MONGO_COLLECTION")]


class ConnectionManager:
    """
    ConnectionManager é responsável por gerenciar as conexões WebSocket ativas,
    enviar notificações para os clientes conectados e lidar com desconexões.

    Feito para organização geral do código e que a lógica de WebSocket 
    não fique bagunçada/misturada com o resto do código.
    """

    def __init__(self):
        self.conexoes_ativas: set[WebSocket] = set()

    async def conectar(self, websocket: WebSocket):
        await websocket.accept()
        self.conexoes_ativas.add(websocket)

    def desconectar(self, websocket: WebSocket):
        if websocket in self.conexoes_ativas:
            self.conexoes_ativas.remove(websocket)

    async def broadcast_licitacao(self, data: dict):
        """
        Envia os dados da licitação para todos os clientes conectados.
        (Notificação e Alertas)
        """
        clientes_desconectados = []
        for conexao in self.conexoes_ativas:
            try:
                await conexao.send_json(data)
            except Exception as e:
                print(f"Erro ao enviar dados para um cliente: {e}")
                clientes_desconectados.append(conexao)

        for conexao in clientes_desconectados:
            self.desconectar(conexao)


async def monitorar_novas_licitacoes():
    """
    Monitora o MongoDB para procurar novas licitações.
    Quando o ETL salvar algo, ele detecta e avisa ao Manager.
    """
    try:
        async with collection.watch([{"$match": {"operationType": "insert"}}]) as stream:
            async for change in stream:
                nova_licitacao = change["fullDocument"]
                nova_licitacao["_id"] = str(nova_licitacao["_id"])

                municipio = nova_licitacao.get("municipioNome", "")

                if municipio.upper() == "RECIFE":
                    await manager.broadcast_licitacao(nova_licitacao)
    except Exception as e:
        print(f"Erro ao monitorar novas licitações: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(monitorar_novas_licitacoes())
    yield


app = FastAPI(lifespan=lifespan)
manager = ConnectionManager()


@app.websocket("/ws/notificacoes")
async def websocket_endpoint(websocket: WebSocket):
    await manager.conectar(websocket)
    try:
        while True:
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.desconectar(websocket)
    except Exception as e:
        print(f"Erro na conexão WebSocket: {e}")
        manager.desconectar(websocket)