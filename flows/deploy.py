"""Registra o deployment `ingestao-vendas` no work pool `vendas-pool`.

Sem schedule: o pipeline é event-driven (disparado por Automation no HANDOFF 04).
Executar de dentro de um container com /app montado e PREFECT_API_URL configurado:
    python /app/flows/deploy.py
"""
import os
from prefect import flow

WORK_POOL = os.getenv("WORK_POOL", "vendas-pool")

if __name__ == "__main__":
    flow.from_source(
        source="/app",
        entrypoint="flows/pipeline.py:processar_arquivo_vendas",
    ).deploy(
        name="ingestao-vendas",
        work_pool_name=WORK_POOL,
    )
    print(f"Deployment 'processar_arquivo_vendas/ingestao-vendas' registrado em {WORK_POOL}.")
