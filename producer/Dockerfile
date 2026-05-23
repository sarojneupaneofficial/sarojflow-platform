FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    pydantic==2.5.3 \
    loguru==0.7.2 \
    click==8.1.7 \
    python-dotenv==1.0.0

COPY producer/ ./producer/
COPY .env.example .env

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "-m", "producer.traffic_producer"]
