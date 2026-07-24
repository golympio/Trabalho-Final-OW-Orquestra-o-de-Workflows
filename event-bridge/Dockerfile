# Ponte MinIO -> Prefect: recebe a notification do MinIO e emite um evento
# customizado no Prefect Server (emit_event). Prefect já está na imagem base.
FROM prefecthq/prefect:3.7.8-python3.12

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /app
COPY app.py /app/app.py

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
