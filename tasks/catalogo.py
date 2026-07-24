"""Catálogo fixo de filiais e produtos (fonte única de verdade).

Usado pela validação da Silver e, futuramente, pelo gerador de vendas (HANDOFF 05).
PRODUTOS: nome -> (categoria, valor_base).
"""

FILIAIS = ["SP01", "SP02", "RJ01", "MG01"]

PRODUTOS = {
    "Notebook": ("Eletrônicos", 3500.00),
    "Monitor":  ("Eletrônicos", 1200.00),
    "Mouse":    ("Acessórios", 80.00),
    "Teclado":  ("Acessórios", 150.00),
    "Headset":  ("Acessórios", 250.00),
    "Cadeira":  ("Móveis", 900.00),
    "Mesa":     ("Móveis", 1200.00),
    "Mochila":  ("Escritório", 180.00),
    "Caderno":  ("Escritório", 25.00),
    "Caneta":   ("Escritório", 5.00),
}


def categoria_de(produto: str):
    item = PRODUTOS.get(produto)
    return item[0] if item else None
