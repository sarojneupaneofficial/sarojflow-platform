FROM apache/spark-py:v3.4.0

USER root

RUN pip install --no-cache-dir \
    delta-spark==3.0.0 \
    pyarrow==14.0.2 \
    pandas==2.1.4 \
    numpy==1.26.2 \
    scikit-learn==1.3.2 \
    joblib==1.3.2 \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    loguru==0.7.2 \
    click==8.1.7 \
    python-dotenv==1.0.0

WORKDIR /app
COPY spark/ ./spark/
COPY .env.example .env

ENV PYTHONPATH=/app
ENV SPARK_JARS_PACKAGES="io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

CMD ["spark-submit", \
     "--packages", "io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", \
     "spark/streaming_job.py"]
