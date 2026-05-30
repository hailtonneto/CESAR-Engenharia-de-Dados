import os
import sys
import asyncio
import json

import streamlit as st
from dotenv import load_dotenv
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from groq import Groq

load_dotenv()

st.set_page_config(
    page_title="Assistente PNCP Recife",
    page_icon="🏛️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .stApp {
        background: linear-gradient(135deg, #0a0a14 0%, #0f0f20 50%, #0a0a14 100%);
        color: #E8E8F0;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d0d1f 0%, #111128 100%);
        border-right: 1px solid rgba(108, 99, 255, 0.2);
    }

    [data-testid="stSidebar"] .stMarkdown { color: #C8C8D8; }

    .sidebar-section {
        background: rgba(108, 99, 255, 0.08);
        border: 1px solid rgba(108, 99, 255, 0.2);
        border-radius: 12px;
        padding: 16px;
        margin-bottom: 16px;
    }

    .sidebar-title {
        font-size: 0.8rem;
        font-weight: 600;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        color: #6C63FF;
        margin-bottom: 12px;
    }

    .main-header {
        text-align: center;
        padding: 40px 20px 30px;
        background: linear-gradient(135deg, rgba(108,99,255,0.12) 0%, rgba(99,179,237,0.08) 100%);
        border-radius: 20px;
        border: 1px solid rgba(108, 99, 255, 0.25);
        margin-bottom: 32px;
        position: relative;
        overflow: hidden;
    }

    .main-header h1 {
        font-size: 2.2rem;
        font-weight: 700;
        background: linear-gradient(135deg, #6C63FF, #63B3ED, #B794F4);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        margin: 0 0 10px 0;
    }

    .main-header p { color: #9B9BB4; font-size: 0.95rem; margin: 0; }

    .user-message {
        display: flex;
        justify-content: flex-end;
        margin: 12px 0;
    }

    .user-bubble {
        background: linear-gradient(135deg, #6C63FF, #8B5CF6);
        color: #fff;
        padding: 12px 18px;
        border-radius: 18px 18px 4px 18px;
        max-width: 75%;
        font-size: 0.95rem;
        line-height: 1.6;
        box-shadow: 0 4px 20px rgba(108, 99, 255, 0.3);
    }

    .assistant-message {
        display: flex;
        align-items: flex-start;
        gap: 12px;
        margin: 12px 0;
    }

    .assistant-avatar {
        width: 38px;
        height: 38px;
        background: linear-gradient(135deg, #1a1a3e, #2a2a5e);
        border: 2px solid rgba(108, 99, 255, 0.5);
        border-radius: 50%;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.1rem;
        flex-shrink: 0;
    }

    .assistant-bubble {
        background: rgba(255,255,255,0.05);
        border: 1px solid rgba(108, 99, 255, 0.2);
        color: #E8E8F0;
        padding: 14px 18px;
        border-radius: 4px 18px 18px 18px;
        max-width: 80%;
        font-size: 0.93rem;
        line-height: 1.7;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.2);
    }

    .tool-call-card {
        background: rgba(108, 99, 255, 0.1);
        border: 1px solid rgba(108, 99, 255, 0.3);
        border-radius: 8px;
        padding: 6px 12px;
        margin-bottom: 8px;
        display: inline-flex;
        align-items: center;
        gap: 8px;
        font-size: 0.8rem;
    }

    .tool-name { color: #A78BFA; font-weight: 600; font-family: 'Courier New', monospace; }
    .tool-detail { color: #6B7280; }

    .thinking-dot {
        display: inline-block;
        width: 8px;
        height: 8px;
        border-radius: 50%;
        background: #6C63FF;
        margin: 0 3px;
        animation: bounce 1.2s infinite ease-in-out;
    }

    .thinking-dot:nth-child(2) { animation-delay: 0.2s; }
    .thinking-dot:nth-child(3) { animation-delay: 0.4s; }

    @keyframes bounce {
        0%, 80%, 100% { transform: scale(0.7); opacity: 0.4; }
        40% { transform: scale(1.1); opacity: 1; }
    }

    .stChatInput > div {
        background: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(108, 99, 255, 0.3) !important;
        border-radius: 16px !important;
    }

    .stChatInput input { color: #E8E8F0 !important; background: transparent !important; }

    .stButton > button {
        background: rgba(108, 99, 255, 0.1) !important;
        border: 1px solid rgba(108, 99, 255, 0.3) !important;
        color: #C8C8E8 !important;
        border-radius: 12px !important;
        font-size: 0.82rem !important;
        transition: all 0.2s ease !important;
        padding: 10px 14px !important;
    }

    .stButton > button:hover {
        background: rgba(108, 99, 255, 0.25) !important;
        border-color: rgba(108, 99, 255, 0.6) !important;
        color: #fff !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 16px rgba(108, 99, 255, 0.3) !important;
    }

    .status-badge {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        background: rgba(16, 185, 129, 0.15);
        border: 1px solid rgba(16, 185, 129, 0.4);
        color: #10B981;
        border-radius: 20px;
        padding: 4px 12px;
        font-size: 0.78rem;
        font-weight: 500;
    }

    .status-dot {
        width: 7px;
        height: 7px;
        border-radius: 50%;
        background: #10B981;
        animation: blink 1.5s infinite;
    }

    @keyframes blink {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.3; }
    }

    ::-webkit-scrollbar { width: 6px; }
    ::-webkit-scrollbar-track { background: rgba(255,255,255,0.02); }
    ::-webkit-scrollbar-thumb { background: rgba(108, 99, 255, 0.4); border-radius: 3px; }

    .stTextInput > div > div > input {
        background: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(108, 99, 255, 0.3) !important;
        border-radius: 10px !important;
        color: #E8E8F0 !important;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

if "messages" not in st.session_state:
    st.session_state.messages = []

if "tool_calls_count" not in st.session_state:
    st.session_state.tool_calls_count = 0

if "total_queries" not in st.session_state:
    st.session_state.total_queries = 0

if "pending_message" not in st.session_state:
    st.session_state.pending_message = None

SUGESTOES = [
    "📊 Quantos registros temos no banco?",
    "💰 Quais as contratações de maior valor?",
    "🏙️ Quais municípios têm mais contratos?",
    "🔍 Busque contratos de tecnologia",
    "📅 Status do pipeline ETL",
    "🌐 Buscar contratos do PNCP em 2025",
]

MCP_SERVER_PARAMS = StdioServerParameters(
    command=sys.executable,
    args=["mcp_server.py"],
    env=dict(os.environ),
)

SYSTEM_PROMPT = (
    "Você é um assistente especializado em contratações públicas do PNCP "
    "(Portal Nacional de Contratações Públicas) com foco em Recife, PE. "
    "Use as ferramentas disponíveis para buscar dados reais. "
    "Formate os valores monetários em reais (R$). "
    "Responda sempre em português brasileiro de forma clara, objetiva e bem estruturada."
)

GROQ_MODEL = "llama-3.3-70b-versatile"

async def fetch_mcp_tools() -> list:
    """Conecta ao servidor MCP e retorna as ferramentas no formato OpenAI/Groq."""
    try:
        async with stdio_client(MCP_SERVER_PARAMS) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools_response = await session.list_tools()
                tools = []
                for tool in tools_response.tools:
                    schema = tool.inputSchema or {}
                    tools.append({
                        "type": "function",
                        "function": {
                            "name": tool.name,
                            "description": tool.description or "",
                            "parameters": {
                                "type": "object",
                                "properties": schema.get("properties", {}),
                                "required": schema.get("required", []),
                            },
                        },
                    })
                return tools
    except Exception as e:
        st.error(f"Erro ao conectar ao servidor MCP: {e}")
        return []

async def call_mcp_tool(tool_name: str, tool_args: dict) -> str:
    """Executa uma ferramenta no servidor MCP e retorna o resultado."""
    try:
        async with stdio_client(MCP_SERVER_PARAMS) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                result = await session.call_tool(tool_name, tool_args)
                if result.content:
                    return result.content[0].text if hasattr(result.content[0], "text") else str(result.content[0])
                return json.dumps({"resultado": "Ferramenta executada sem retorno."})
    except Exception as e:
        return json.dumps({"erro": f"Falha ao executar '{tool_name}': {str(e)}"})

async def chat_with_groq(
    user_message: str,
    history: list,
    tools: list,
) -> tuple[str, list]:
    """
    Envia a mensagem ao Groq (Llama) com suporte a function calling via MCP.
    Retorna (texto_resposta, lista_tool_calls_executadas).
    """
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        return "⚠️ Configure a Groq API Key na barra lateral para usar o assistente.", []

    client = Groq(api_key=api_key)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    for msg in history:
        messages.append({"role": msg["role"], "content": msg["content"]})
    messages.append({"role": "user", "content": user_message})

    executed_tools = []
    max_iterations = 5

    for _ in range(max_iterations):
        kwargs = {
            "model": GROQ_MODEL,
            "messages": messages,
            "max_tokens": 4096,
            "temperature": 0.2,
        }
        if tools:
            kwargs["tools"] = tools
            kwargs["tool_choice"] = "auto"

        response = await asyncio.to_thread(
            client.chat.completions.create,
            **kwargs,
        )

        msg_resp = response.choices[0].message
        if not msg_resp.tool_calls:
            break
        messages.append({
            "role": "assistant",
            "content": msg_resp.content or "",
            "tool_calls": [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                }
                for tc in msg_resp.tool_calls
            ],
        })
        for tc in msg_resp.tool_calls:
            tool_name = tc.function.name
            try:
                tool_args = json.loads(tc.function.arguments)
            except json.JSONDecodeError:
                tool_args = {}

            executed_tools.append({"name": tool_name, "args": tool_args})
            result_str = await call_mcp_tool(tool_name, tool_args)

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": result_str,
            })
    final_text = response.choices[0].message.content or ""
    if not final_text:
        final_text = "Desculpe, não consegui gerar uma resposta. Tente novamente."

    return final_text, executed_tools

with st.sidebar:
    st.markdown(
        """
        <div style="text-align:center;padding:20px 0 8px;">
            <div style="font-size:3rem;">🏛️</div>
            <div style="font-weight:700;font-size:1.1rem;color:#E8E8F0;margin-top:8px;">
                PNCP Recife
            </div>
            <div style="margin-top:8px;">
                <span class="status-badge">
                    <span class="status-dot"></span>
                    Online
                </span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.divider()

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-title">📈 Sessão Atual</div>', unsafe_allow_html=True)
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Consultas", st.session_state.total_queries)
    with col2:
        st.metric("Tools usadas", st.session_state.tool_calls_count)
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-title">🔑 Configuração</div>', unsafe_allow_html=True)
    api_input = st.text_input(
        "Groq API Key",
        value=os.getenv("GROQ_API_KEY") or "",
        type="password",
        help="Obtenha gratuitamente em console.groq.com",
        placeholder="gsk_...",
    )
    if api_input:
        os.environ["GROQ_API_KEY"] = api_input
    st.markdown(
        '<a href="https://console.groq.com/keys" target="_blank" '
        'style="color:#6C63FF;font-size:0.78rem;">→ Obter chave gratuita em console.groq.com</a>',
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-title">🔧 Ferramentas MCP</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div style="font-size:0.8rem;color:#9B9BB4;line-height:2.0">
        🔎 buscar_contratacoes_pncp<br>
        🗄️ consultar_mongodb<br>
        📊 resumir_contratacoes_mongodb<br>
        🔢 buscar_contratacao_por_numero<br>
        🏙️ listar_municipios_disponiveis<br>
        💰 buscar_contratacoes_por_valor<br>
        📝 buscar_contratacoes_por_objeto<br>
        ⚙️ status_pipeline
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
    st.markdown('<div class="sidebar-title">ℹ️ Sobre</div>', unsafe_allow_html=True)
    st.markdown(
        """
        <div style="font-size:0.82rem;color:#9B9BB4;line-height:1.7">
        Pipeline ETL de contratações públicas<br>
        do PNCP para Recife/PE.<br><br>
        <b style="color:#E8E8F0">Modelo:</b> Llama 3.3 70B (Groq)<br>
        <b style="color:#E8E8F0">Protocolo:</b> MCP via STDIO<br>
        <b style="color:#E8E8F0">Dados:</b> MongoDB Atlas + PNCP API
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("</div>", unsafe_allow_html=True)

    if st.button("🗑️ Limpar conversa", use_container_width=True):
        st.session_state.messages = []
        st.session_state.tool_calls_count = 0
        st.session_state.total_queries = 0
        st.session_state.pending_message = None
        st.rerun()

st.markdown(
    """
    <div class="main-header">
        <h1>🏛️ Assistente PNCP Recife</h1>
        <p>Consulte contratações públicas com inteligência artificial • Pipeline ETL • MongoDB + PNCP API</p>
    </div>
    """,
    unsafe_allow_html=True,
)

if not st.session_state.messages:
    st.markdown(
        """
        <div style="text-align:center;padding:32px 16px 16px;">
            <div style="font-size:3rem;margin-bottom:12px;">🤖</div>
            <h3 style="color:#E8E8F0;margin:0 0 8px 0;">Como posso ajudar?</h3>
            <p style="color:#9B9BB4;font-size:0.9rem;margin-bottom:24px;">
                Faça perguntas sobre contratações públicas de Recife ou experimente uma sugestão:
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    cols = st.columns(3)
    for i, sugestao in enumerate(SUGESTOES):
        with cols[i % 3]:
            if st.button(sugestao, key=f"sug_{i}", use_container_width=True):
                st.session_state.pending_message = sugestao
                st.rerun()

for msg in st.session_state.messages:
    if msg["role"] == "user":
        st.markdown(
            f'<div class="user-message"><div class="user-bubble">{msg["content"]}</div></div>',
            unsafe_allow_html=True,
        )
    else:
        tool_html = ""
        for tc in msg.get("tool_calls", []):
            tool_html += (
                f'<div class="tool-call-card">'
                f'<span>🔧</span>'
                f'<span class="tool-name">{tc["name"]}</span>'
                f'<span class="tool-detail">executada</span>'
                f"</div>"
            )
        st.markdown(
            f'<div class="assistant-message">'
            f'<div class="assistant-avatar">🤖</div>'
            f'<div class="assistant-bubble">{tool_html}{msg["content"]}</div>'
            f"</div>",
            unsafe_allow_html=True,
        )

user_input = st.chat_input("Digite sua pergunta sobre contratações públicas...")

active_input = user_input or st.session_state.pending_message

if active_input:
    st.session_state.pending_message = None
    st.session_state.messages.append({"role": "user", "content": active_input})
    st.session_state.total_queries += 1

    st.markdown(
        f'<div class="user-message"><div class="user-bubble">{active_input}</div></div>',
        unsafe_allow_html=True,
    )

    thinking_placeholder = st.empty()
    thinking_placeholder.markdown(
        '<div class="assistant-message">'
        '<div class="assistant-avatar">🤖</div>'
        '<div class="assistant-bubble">'
        '<span class="thinking-dot"></span>'
        '<span class="thinking-dot"></span>'
        '<span class="thinking-dot"></span>'
        "</div></div>",
        unsafe_allow_html=True,
    )

    history_for_api = [
        m for m in st.session_state.messages[:-1]
        if m["role"] in ("user", "assistant")
    ]

    try:
        tools = asyncio.run(fetch_mcp_tools())
        response_text, tool_calls = asyncio.run(
            chat_with_groq(active_input, history_for_api, tools)
        )
    except Exception as e:
        err = str(e)
        tool_calls = []
        if "401" in err or "invalid_api_key" in err.lower() or "authentication" in err.lower():
            response_text = (
                "⚠️ **Chave de API inválida.**\n\n"
                "Verifique sua Groq API Key na barra lateral. "
                "Ela deve começar com `gsk_`. "
                "Obtenha uma gratuitamente em [console.groq.com](https://console.groq.com/keys)."
            )
        elif "429" in err or "rate_limit" in err.lower():
            response_text = (
                "⚠️ **Limite de requisições atingido.**\n\n"
                "Aguarde alguns instantes e tente novamente."
            )
        else:
            response_text = f"❌ **Erro inesperado:** `{e}`"

    st.session_state.tool_calls_count += len(tool_calls)
    thinking_placeholder.empty()

    st.session_state.messages.append(
        {"role": "assistant", "content": response_text, "tool_calls": tool_calls}
    )

    tool_html = ""
    for tc in tool_calls:
        tool_html += (
            f'<div class="tool-call-card">'
            f'<span>🔧</span>'
            f'<span class="tool-name">{tc["name"]}</span>'
            f'<span class="tool-detail">executada</span>'
            f"</div>"
        )

    st.markdown(
        f'<div class="assistant-message">'
        f'<div class="assistant-avatar">🤖</div>'
        f'<div class="assistant-bubble">{tool_html}{response_text}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )