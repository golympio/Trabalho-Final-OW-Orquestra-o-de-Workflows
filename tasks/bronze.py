"""Camada Bronze: registro de controle (idempotência de arquivo) e preservação
do arquivo bruto imutável no MinIO."""
from datetime import date

from prefect import task, get_run_logger
from prefect.runtime import flow_run

from tasks.config import BRONZE_BUCKET
from tasks.storage import baixar_bytes, enviar_bytes, sha256_hex
from tasks.db import conectar


def origem_de(key: str) -> str:
    """Extrai a origem do nome: VENDAS_<ORIGEM>_<AAAAMMDD>[_<MARCADOR>]_<seq>.csv."""
    nome = key.rsplit("/", 1)[-1]
    partes = nome.split("_")
    return partes[1] if len(partes) >= 2 else "DESCONHECIDA"


@task(retries=2, retry_delay_seconds=3)
def registrar_arquivo(bucket: str, key: str) -> dict:
    """Baixa o objeto, calcula sha256 e registra em bronze_arquivos.
    Idempotência de ARQUIVO: sha256 UNIQUE. Se já existir e estiver 'concluido',
    sinaliza duplicado (short-circuit)."""
    log = get_run_logger()
    data = baixar_bytes(bucket, key)
    sha = sha256_hex(data)
    origem = origem_de(key)
    nome = key.rsplit("/", 1)[-1]
    run_id = flow_run.get_id()

    with conectar() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO bronze_arquivos
                   (original_name, origem, sha256, size_bytes, status, flow_run_id)
               VALUES (%s, %s, %s, %s, 'processando', %s)
               ON CONFLICT (sha256) DO NOTHING
               RETURNING file_id""",
            (nome, origem, sha, len(data), run_id),
        )
        row = cur.fetchone()
        if row:
            file_id, duplicado = row[0], False
        else:
            cur.execute("SELECT file_id, status FROM bronze_arquivos WHERE sha256 = %s", (sha,))
            file_id, status = cur.fetchone()
            duplicado = (status == "concluido")
        conn.commit()

    if duplicado:
        log.info(f"Arquivo já processado (sha256={sha[:12]}...). Short-circuit por idempotência.")
    else:
        log.info(f"Arquivo registrado: origem={origem} sha256={sha[:12]}... file_id={file_id}")

    return {
        "file_id": str(file_id),
        "sha256": sha,
        "origem": origem,
        "duplicado": duplicado,
        "data": data,
    }


@task(retries=2, retry_delay_seconds=3)
def preservar_bronze(file_id: str, key: str, origem: str, data: bytes, sha256: str) -> str:
    """Copia os bytes originais, imutáveis, para o bucket bronze."""
    ingest = date.today().isoformat()
    nome = key.rsplit("/", 1)[-1]
    bronze_key = f"ingest_date={ingest}/origem={origem}/{sha256}__{nome}"
    enviar_bytes(BRONZE_BUCKET, bronze_key, data)
    uri = f"s3://{BRONZE_BUCKET}/{bronze_key}"
    with conectar() as conn, conn.cursor() as cur:
        cur.execute("UPDATE bronze_arquivos SET bronze_uri = %s WHERE file_id = %s", (uri, file_id))
        conn.commit()
    get_run_logger().info(f"Bronze preservado: {uri}")
    return uri
