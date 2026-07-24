"""Configuração via variáveis de ambiente (defaults = docker-compose)."""
import os

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
LANDING_BUCKET = os.getenv("LANDING_BUCKET", "landing")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")

PG = {
    "host": os.getenv("PGHOST", "postgres"),
    "dbname": os.getenv("PGDATABASE", "vendas"),
    "user": os.getenv("PGUSER", "puc"),
    "password": os.getenv("PGPASSWORD", "puc"),
}
