# Imagem do worker/execução do pipeline: Prefect + clientes MinIO e PostgreSQL.
# O código-fonte é montado em /app via volume (ver docker-compose.yml).
FROM prefecthq/prefect:3.7.8-python3.12

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
