FROM python:3.10-slim

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app
COPY .env /app/.env

ENV DAGSTER_HOME=/app
EXPOSE 3000

CMD ["dagster", "dev", "-f", "/app/etl_pipeline/definitions.py", "-h", "0.0.0.0", "-p", "3000"]
