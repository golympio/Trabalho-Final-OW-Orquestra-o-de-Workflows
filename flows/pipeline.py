"""Flow event-driven da arquitetura Medalhão: Bronze -> Silver -> Gold.

Disparado (HANDOFF 04) por Automation ao chegar um arquivo no MinIO; nesta etapa
(HANDOFF 03) é validado por execução manual parametrizada.
"""
from prefect import flow, task, get_run_logger
from prefect.context import get_run_context
from prefect.artifacts import create_markdown_artifact

from tasks.bronze import registrar_arquivo, preservar_bronze, origem_de
from tasks.silver import validar_silver, gravar_silver
from tasks.gold import atualizar_gold
from tasks.db import conectar


@task(retries=2, retry_delay_seconds=2)
def checkpoint_falha_controlada(marcado: bool) -> str:
    """RECURSO DE TESTE (não é regra de produção): quando o arquivo tem o marcador
    _FALHA_ no nome, falha nas 2 primeiras tentativas e conclui na 3ª (retries=2),
    para demonstrar os retries do Prefect na UI."""
    if not marcado:
        return "sem_falha"
    log = get_run_logger()
    tentativa = get_run_context().task_run.run_count
    if tentativa <= 2:
        log.warning(f"[TESTE] Falha transitória controlada (tentativa {tentativa}/3).")
        raise RuntimeError(f"[TESTE] Falha controlada na tentativa {tentativa} (marcador _FALHA_).")
    log.info(f"[TESTE] Sucesso na tentativa {tentativa}.")
    return f"sucesso_tentativa_{tentativa}"


def _publicar_resumo(file_id, key, duplicado, validas=0, rejeitadas=0):
    with conectar() as conn, conn.cursor() as cur:
        cur.execute("SELECT faturamento_total, quantidade_vendas, ticket_medio "
                    "FROM gold_kpis_gerais WHERE id = 1")
        kpi = cur.fetchone()
    fat, qtd, ticket = kpi if kpi else (0, 0, 0)
    md = (
        f"# Resumo da ingestão — `{key}`\n\n"
        f"- **file_id:** `{file_id}`\n"
        f"- **duplicado:** {duplicado}\n"
        f"- **válidas:** {validas} | **rejeitadas:** {rejeitadas}\n\n"
        f"## Gold — KPIs gerais\n"
        f"- Faturamento total: **R$ {fat}**\n"
        f"- Quantidade de vendas: **{qtd}**\n"
        f"- Ticket médio: **R$ {ticket}**\n"
    )
    create_markdown_artifact(markdown=md, key="resumo-ingestao",
                             description=f"Resumo da ingestão de {key}")


@flow(name="processar_arquivo_vendas", flow_run_name="ingestao-{key}", log_prints=True)
def processar_arquivo_vendas(bucket: str, key: str, origem: str | None = None) -> dict:
    """Processa um arquivo de vendas: Bronze (registro+preservação) -> Silver
    (validação/dedup) -> Gold (KPIs), com idempotência e rastreabilidade."""
    log = get_run_logger()
    origem = origem or origem_de(key)
    marcado_falha = "_FALHA_" in key.upper()
    log.info(f"Iniciando ingestão: bucket={bucket} key={key} origem={origem}")

    reg = registrar_arquivo(bucket, key)
    if reg["duplicado"]:
        log.info("Arquivo duplicado (idempotência de arquivo): nada a reprocessar.")
        _publicar_resumo(reg["file_id"], key, duplicado=True)
        return {"status": "duplicado", "file_id": reg["file_id"]}

    # Cenário controlado de resiliência (só dispara com o marcador _FALHA_).
    checkpoint_falha_controlada(marcado_falha)

    preservar_bronze(reg["file_id"], key, origem, reg["data"], reg["sha256"])
    val = validar_silver(reg["data"], origem)
    grav = gravar_silver(reg["file_id"], val["validas"], val["rejeitadas"])
    atualizar_gold()

    _publicar_resumo(reg["file_id"], key, duplicado=False,
                     validas=grav["validas"], rejeitadas=grav["rejeitadas"])
    log.info(f"Ingestão concluída: {grav}")
    return {"status": "ok", "file_id": reg["file_id"], **grav}


if __name__ == "__main__":
    import sys
    # Execução local para depuração: python flows/pipeline.py <bucket> <key>
    processar_arquivo_vendas(sys.argv[1], sys.argv[2])
