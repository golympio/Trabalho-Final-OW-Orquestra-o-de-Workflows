"""Ponte MinIO -> Prefect.

Recebe a bucket notification do MinIO (POST /minio-events), extrai bucket/key,
deriva a origem e **emite um evento customizado** `vendas.arquivo.recebido` no
Prefect Server. Uma Automation reage a esse evento e dispara o deployment.

Modo de disparo (BRIDGE_TRIGGER_MODE):
  - "automation" (padrão): apenas emite o evento; a Automation faz o run.
  - "direct": além de emitir, chama run_deployment diretamente (fallback do GATE
    §3.3.1, caso o templating de parâmetros da Automation não seja estável).
"""
import os
from urllib.parse import unquote_plus

from fastapi import FastAPI, Request
from prefect.events import emit_event

app = FastAPI(title="event-bridge MinIO->Prefect")

TRIGGER_MODE = os.getenv("BRIDGE_TRIGGER_MODE", "automation")
DEPLOYMENT = os.getenv("BRIDGE_DEPLOYMENT", "processar_arquivo_vendas/ingestao-vendas")
EVENTO = os.getenv("EVENTO_VENDAS", "vendas.arquivo.recebido")


def origem_de(key: str) -> str:
    nome = key.rsplit("/", 1)[-1]
    partes = nome.split("_")
    return partes[1] if len(partes) >= 2 else "DESCONHECIDA"


@app.get("/health")
def health():
    return {"status": "ok", "mode": TRIGGER_MODE}


@app.post("/minio-events")
async def minio_events(request: Request):
    body = await request.json()
    processados = []
    for rec in body.get("Records", []):
        s3 = rec.get("s3", {})
        bucket = (s3.get("bucket") or {}).get("name")
        key = (s3.get("object") or {}).get("key")
        if not bucket or not key:
            continue
        key = unquote_plus(key)
        origem = origem_de(key)
        object_id = (s3.get("object") or {}).get("eTag")

        emit_event(
            event=EVENTO,
            resource={"prefect.resource.id": f"minio.object.{bucket}/{key}"},
            payload={"bucket": bucket, "key": key, "origem": origem, "object_id": object_id},
        )

        if TRIGGER_MODE == "direct":
            from prefect.deployments import run_deployment
            await run_deployment(
                name=DEPLOYMENT,
                parameters={"bucket": bucket, "key": key, "origem": origem},
                timeout=0,
            )

        processados.append({"bucket": bucket, "key": key, "origem": origem})

    return {"mode": TRIGGER_MODE, "processados": processados}
