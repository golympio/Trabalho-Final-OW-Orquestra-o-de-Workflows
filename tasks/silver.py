"""Camada Silver: validação, limpeza, tipagem, deduplicação e separação de
registros válidos x rejeitados; gravação idempotente."""
import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation

from prefect import task, get_run_logger

from tasks.catalogo import PRODUTOS
from tasks.db import conectar

CAMPOS = ["venda_id_origem", "data_venda", "filial", "produto", "categoria",
          "quantidade", "valor_unitario"]


def _validar_linha(row: dict, origem: str):
    """Retorna (valido, dados|None, motivo|None, detalhe|None). Reporta o
    primeiro erro encontrado (a ordem cobre os 5 tipos definidos)."""
    vid = (row.get("venda_id_origem") or "").strip()
    if not vid:
        return False, None, "venda_id_origem_ausente", None

    try:
        dv = datetime.strptime((row.get("data_venda") or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        return False, None, "data_venda_invalida", row.get("data_venda")

    try:
        q = int((row.get("quantidade") or "").strip())
        if q <= 0:
            raise ValueError
    except ValueError:
        return False, None, "quantidade_invalida", row.get("quantidade")

    try:
        vu = Decimal((row.get("valor_unitario") or "").strip())
        if vu <= 0:
            raise InvalidOperation
    except (InvalidOperation, ArithmeticError):
        return False, None, "valor_unitario_invalido", row.get("valor_unitario")

    produto = (row.get("produto") or "").strip()
    categoria = (row.get("categoria") or "").strip()
    if produto not in PRODUTOS or PRODUTOS[produto][0] != categoria:
        return False, None, "produto_invalido_ou_incompativel", f"{produto}/{categoria}"

    vt = (q * vu).quantize(Decimal("0.01"))
    dados = {
        "origem": origem,
        "filial": (row.get("filial") or "").strip(),
        "categoria": categoria,
        "produto": produto,
        "venda_id_origem": vid,
        "data_venda": dv,
        "quantidade": q,
        "valor_unitario": vu,
        "valor_total": vt,
    }
    return True, dados, None, None


@task
def validar_silver(data: bytes, origem: str) -> dict:
    """Faz parse do CSV, valida/limpa/tipa e separa válidos x rejeitados.
    Deduplica dentro do arquivo por (origem, venda_id_origem)."""
    texto = data.decode("utf-8")
    reader = csv.DictReader(io.StringIO(texto))
    validas, rejeitadas = [], []
    vistos = set()

    for i, row in enumerate(reader, start=2):  # linha 1 = header
        raw = ",".join((row.get(c) or "") for c in CAMPOS)
        ok, dados, motivo, detalhe = _validar_linha(row, origem)
        if not ok:
            rejeitadas.append({"linha": i, "payload": raw, "motivo": motivo, "detalhe": detalhe})
            continue
        chave = (dados["origem"], dados["venda_id_origem"])
        if chave in vistos:
            rejeitadas.append({"linha": i, "payload": raw,
                               "motivo": "duplicado_no_arquivo", "detalhe": dados["venda_id_origem"]})
            continue
        vistos.add(chave)
        dados["linha_origem"] = i
        validas.append(dados)

    get_run_logger().info(f"Silver: {len(validas)} válidas, {len(rejeitadas)} rejeitadas.")
    return {"validas": validas, "rejeitadas": rejeitadas}


@task(retries=2, retry_delay_seconds=3)
def gravar_silver(file_id: str, validas: list, rejeitadas: list) -> dict:
    """Upsert das válidas (idempotência de registro) + registro dos rejeitados +
    atualização das métricas/estado do arquivo na Bronze-controle."""
    with conectar() as conn, conn.cursor() as cur:
        for d in validas:
            cur.execute(
                """INSERT INTO silver_vendas
                       (file_id, origem, filial, categoria, produto, venda_id_origem,
                        data_venda, quantidade, valor_unitario, valor_total, linha_origem)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                   ON CONFLICT (origem, venda_id_origem) DO NOTHING""",
                (file_id, d["origem"], d["filial"], d["categoria"], d["produto"],
                 d["venda_id_origem"], d["data_venda"], d["quantidade"],
                 d["valor_unitario"], d["valor_total"], d["linha_origem"]),
            )
        for r in rejeitadas:
            cur.execute(
                """INSERT INTO silver_vendas_rejeitadas
                       (file_id, linha_origem, payload_raw, motivo, detalhe)
                   VALUES (%s,%s,%s,%s,%s)""",
                (file_id, r["linha"], r["payload"], r["motivo"], r.get("detalhe")),
            )
        total = len(validas) + len(rejeitadas)
        cur.execute(
            """UPDATE bronze_arquivos
                   SET status='concluido', linhas_total=%s, linhas_validas=%s, linhas_rejeitadas=%s
                 WHERE file_id=%s""",
            (total, len(validas), len(rejeitadas), file_id),
        )
        conn.commit()
    return {"validas": len(validas), "rejeitadas": len(rejeitadas), "total": total}
