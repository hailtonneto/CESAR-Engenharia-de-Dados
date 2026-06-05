"""
Configuração do pytest.

Garante que a raiz do subprojeto (CESAR-Engenharia-de-Dados) esteja no
``sys.path``, de modo que ``import src...`` e ``import orchestrate_prefect``
funcionem ao rodar ``python -m pytest`` de qualquer diretório.
"""
import os
import sys

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if RAIZ not in sys.path:
    sys.path.insert(0, RAIZ)
