FROM python:3.11-slim-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /app/models

ENV PYTHONUNBUFFERED=1
ENV FLASK_PORT=8084
ENV FEATURE_SCHEMA_PATH=/app/feature-schema.json
ENV GUNICORN_WORKERS=2
ENV GUNICORN_THREADS=4

EXPOSE 8084

CMD gunicorn \
    --workers ${GUNICORN_WORKERS} \
    --threads ${GUNICORN_THREADS} \
    --bind 0.0.0.0:${FLASK_PORT} \
    --timeout 120 \
    --access-logfile - \
    --error-logfile - \
    "wsgi:app"
