#!/usr/bin/env bash
# Envia um arquivo de vendas para o bucket `landing` do MinIO, disparando o
# pipeline event-driven. A chegada do objeto gera a notification -> event-bridge
# -> evento -> Automation -> deployment.
#
# Uso: ./scripts/enviar_arquivo.sh samples/VENDAS_SP01_20260723_001.csv
set -euo pipefail

ARQ="${1:-}"
if [[ -z "$ARQ" || ! -f "$ARQ" ]]; then
  echo "uso: $0 <caminho-do-csv>   (ex.: samples/VENDAS_SP01_20260723_001.csv)" >&2
  exit 1
fi

BASENAME="$(basename "$ARQ")"
DIR="$(cd "$(dirname "$ARQ")" && pwd)"
LANDING="${LANDING_BUCKET:-landing}"

# Usa o cliente mc já pinado no compose (serviço minio-init), montando o arquivo.
docker compose run --rm --no-deps -v "$DIR:/seed:ro" --entrypoint sh minio-init -c "
  mc alias set local http://minio:9000 \"\$MINIO_ROOT_USER\" \"\$MINIO_ROOT_PASSWORD\" >/dev/null
  mc cp \"/seed/$BASENAME\" \"local/$LANDING/$BASENAME\"
"
echo "enviado: $BASENAME -> bucket $LANDING"
