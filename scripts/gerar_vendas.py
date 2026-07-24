#!/usr/bin/env python3
"""Gerador determinístico de arquivos de vendas fictícios (§3.9 do PRD).

- Cada execução gera um CSV de UMA única filial (--origem); a coluna `filial`
  repete a origem em todas as linhas.
- Reprodutibilidade byte-a-byte: UTF-8, ordem estável de colunas, separador `,`
  e fim de linha `\\n`, valores monetários com 2 casas, sem data/uuid dependente
  do momento da execução (a data vem de --data).
- Mesma seed + mesmos parâmetros => mesmo arquivo byte-a-byte.

Uso:
  python3 scripts/gerar_vendas.py --origem SP01 --cenario valido --seed 101
  python3 scripts/gerar_vendas.py --origem SP01 --cenario invalido --seq 002 --seed 201

Sem dependências externas (apenas biblioteca padrão). O catálogo é importado de
tasks/catalogo.py (fonte única de verdade).
"""
import argparse
import csv
import hashlib
import os
import sys
from decimal import Decimal
from random import Random

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from tasks.catalogo import FILIAIS, PRODUTOS  # noqa: E402

CAMPOS = ["venda_id_origem", "data_venda", "filial", "produto", "categoria",
          "quantidade", "valor_unitario"]
CEM_PCT = Decimal(10000)


def _valor_unitario(base: float, rng: Random) -> str:
    """Valor-base com variação determinística de -10% a +10%, 2 casas decimais.
    Usa basis points inteiros (RNG inteiro) para reprodutibilidade cross-platform."""
    bp = rng.randint(-1000, 1000)  # -10.00% .. +10.00%
    valor = (Decimal(str(base)) * (CEM_PCT + bp) / CEM_PCT).quantize(Decimal("0.01"))
    return f"{valor:.2f}"


def _venda_valida(origem, data_iso, data_str, disc, idx, rng: Random) -> list:
    produto = rng.choice(list(PRODUTOS.keys()))
    categoria, base = PRODUTOS[produto]
    quantidade = rng.randint(1, 5)
    # `disc` (marcador+seq) torna venda_id_origem único por origem ENTRE arquivos
    vid = f"{origem}-{data_str}-{disc}-{idx:03d}"
    return [vid, data_iso, origem, produto, categoria, str(quantidade), _valor_unitario(base, rng)]


def gerar_linhas(origem, data_str, seq, cenario, linhas, rng):
    data_iso = f"{data_str[0:4]}-{data_str[4:6]}-{data_str[6:8]}"
    disc = f"FALHA{seq}" if cenario == "falha" else seq
    v = lambda i: _venda_valida(origem, data_iso, data_str, disc, i, rng)  # noqa: E731

    if cenario in ("valido", "falha"):
        return [v(i) for i in range(1, linhas + 1)]

    if cenario == "invalido":
        n_val = linhas - 5  # 15 válidas + 5 inválidas (uma por tipo de erro)
        rows = [v(i) for i in range(1, n_val + 1)]
        e1 = v(n_val + 1); e1[0] = ""                                  # venda_id_origem ausente
        e2 = v(n_val + 2); e2[1] = "31/07/2026"                        # data_venda inválida
        e3 = v(n_val + 3); e3[5] = "0"                                 # quantidade <= 0
        e4 = v(n_val + 4); e4[6] = "0.00"                              # valor_unitario <= 0
        e5 = v(n_val + 5); e5[3] = "Geladeira"; e5[4] = "Eletrônicos"  # produto inexistente
        return rows + [e1, e2, e3, e4, e5]

    if cenario == "duplicado":
        n_uniq = linhas - 2  # 18 chaves únicas + 2 repetições da mesma chave
        rows = [v(i) for i in range(1, n_uniq + 1)]
        return rows + [list(rows[0]), list(rows[1])]

    raise ValueError(f"cenário inválido: {cenario}")


def nome_arquivo(origem, data_str, cenario, seq):
    marcador = "_FALHA" if cenario == "falha" else ""
    return f"VENDAS_{origem}_{data_str}{marcador}_{seq}.csv"


def main():
    ap = argparse.ArgumentParser(description="Gerador determinístico de vendas fictícias (§3.9).")
    ap.add_argument("--origem", required=True, choices=FILIAIS, help="filial (uma por arquivo)")
    ap.add_argument("--cenario", default="valido",
                    choices=["valido", "invalido", "duplicado", "falha"])
    ap.add_argument("--linhas", type=int, default=20)
    ap.add_argument("--data", default="20260723", help="data do lote (AAAAMMDD)")
    ap.add_argument("--seq", default="001", help="sequencial de 3 dígitos")
    ap.add_argument("--seed", type=int, default=None, help="semente; se omitida, derivada dos parâmetros")
    ap.add_argument("--saida", default="samples", help="diretório de saída")
    args = ap.parse_args()

    if args.seed is not None:
        seed = args.seed
    else:
        # Semente determinística (hashlib, não hash() que é randomizado por processo).
        chave = f"{args.origem}|{args.data}|{args.seq}|{args.cenario}|{args.linhas}"
        seed = int(hashlib.sha256(chave.encode()).hexdigest(), 16) % (2**32)
    rng = Random(seed)

    linhas = gerar_linhas(args.origem, args.data, args.seq, args.cenario, args.linhas, rng)
    nome = nome_arquivo(args.origem, args.data, args.cenario, args.seq)
    os.makedirs(args.saida, exist_ok=True)
    caminho = os.path.join(args.saida, nome)
    with open(caminho, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(CAMPOS)
        w.writerows(linhas)
    print(f"gerado: {caminho} ({len(linhas)} linhas, cenario={args.cenario}, seed={seed})")


if __name__ == "__main__":
    main()
