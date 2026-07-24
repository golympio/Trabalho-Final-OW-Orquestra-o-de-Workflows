"""Camada Gold: recomputação idempotente dos indicadores a partir da Silver.
Mesma lógica de sql/gold_refresh.sql (KPIs por upsert; dimensões por full-refresh)."""
from prefect import task, get_run_logger

from tasks.db import conectar

_KPIS = """
INSERT INTO gold_kpis_gerais (id, faturamento_total, quantidade_vendas, ticket_medio)
SELECT 1, COALESCE(SUM(valor_total),0), COUNT(*),
       CASE WHEN COUNT(*)=0 THEN 0 ELSE ROUND(SUM(valor_total)/COUNT(*),2) END
FROM silver_vendas
ON CONFLICT (id) DO UPDATE SET
    faturamento_total = EXCLUDED.faturamento_total,
    quantidade_vendas = EXCLUDED.quantidade_vendas,
    ticket_medio      = EXCLUDED.ticket_medio,
    atualizado_em     = now();
"""

_DIMENSOES = [
    ("gold_vendas_por_filial",
     "INSERT INTO gold_vendas_por_filial (filial, faturamento, quantidade_vendas, ticket_medio) "
     "SELECT filial, SUM(valor_total), COUNT(*), ROUND(SUM(valor_total)/COUNT(*),2) "
     "FROM silver_vendas GROUP BY filial"),
    ("gold_vendas_por_categoria",
     "INSERT INTO gold_vendas_por_categoria (categoria, faturamento, quantidade_vendas, ticket_medio) "
     "SELECT categoria, SUM(valor_total), COUNT(*), ROUND(SUM(valor_total)/COUNT(*),2) "
     "FROM silver_vendas GROUP BY categoria"),
    ("gold_vendas_por_produto",
     "INSERT INTO gold_vendas_por_produto (produto, categoria, faturamento, quantidade_vendas, ticket_medio) "
     "SELECT produto, categoria, SUM(valor_total), COUNT(*), ROUND(SUM(valor_total)/COUNT(*),2) "
     "FROM silver_vendas GROUP BY produto, categoria"),
]


@task(retries=2, retry_delay_seconds=3)
def atualizar_gold() -> None:
    """Recomputa a Gold de forma determinística/idempotente a partir da Silver."""
    with conectar() as conn, conn.cursor() as cur:
        cur.execute(_KPIS)
        for tabela, insert_sql in _DIMENSOES:
            cur.execute(f"TRUNCATE {tabela}")
            cur.execute(insert_sql)
        conn.commit()
    get_run_logger().info("Gold recomputada.")
