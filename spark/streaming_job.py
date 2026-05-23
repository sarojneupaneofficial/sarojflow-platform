"""
sarojflow.spark.streaming_job
──────────────────────────────
PySpark Structured Streaming pipeline.

Reads raw JSON events from Kafka topic `traffic.raw.events`,
validates schema, cleans bad records, computes congestion scores,
and writes to three Delta Lake zones:

    data/delta/raw/         — unmodified, append-only
    data/delta/clean/       — validated, enriched
    data/delta/analytics/   — hourly aggregations

Usage (inside Spark container):
    spark-submit \
        --packages io.delta:delta-core_2.12:2.4.0,\
                   org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        spark/streaming_job.py
"""

from __future__ import annotations

import os
import sys

from delta import DeltaTable, configure_spark_with_delta_pip
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ─── Config ────────────────────────────────────────────────────────────────────

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC   = os.getenv("KAFKA_TOPIC_RAW", "traffic.raw.events")
DELTA_BASE    = os.getenv("DELTA_LAKE_PATH", "./data/delta")

RAW_PATH       = f"{DELTA_BASE}/raw"
CLEAN_PATH     = f"{DELTA_BASE}/clean"
ANALYTICS_PATH = f"{DELTA_BASE}/analytics"
CHECKPOINT_RAW     = f"{DELTA_BASE}/_checkpoints/raw"
CHECKPOINT_CLEAN   = f"{DELTA_BASE}/_checkpoints/clean"
CHECKPOINT_AGGR    = f"{DELTA_BASE}/_checkpoints/analytics"

# ─── Spark Session ─────────────────────────────────────────────────────────────

builder = (
    SparkSession.builder
    .appName("SarojFlow-TrafficStreaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.shuffle.partitions", "12")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("✓ SparkSession initialised — SarojFlow Streaming Pipeline v2.4.1")

# ─── Event Schema ──────────────────────────────────────────────────────────────

TRAFFIC_SCHEMA = StructType([
    StructField("event_id",          StringType(),    False),
    StructField("camera_id",         StringType(),    False),
    StructField("timestamp",         StringType(),    False),  # ISO string from Kafka
    StructField("location",          StringType(),    True),
    StructField("latitude",          DoubleType(),    True),
    StructField("longitude",         DoubleType(),    True),
    StructField("vehicle_count",     IntegerType(),   False),
    StructField("average_speed",     DoubleType(),    False),
    StructField("congestion_level",  StringType(),    False),
    StructField("congestion_score",  DoubleType(),    False),
    StructField("accident_detected", BooleanType(),   True),
    StructField("pedestrian_count",  IntegerType(),   True),
    StructField("heavy_vehicle_pct", DoubleType(),    True),
    StructField("producer_version",  StringType(),    True),
    StructField("schema_version",    StringType(),    True),
])

# ─── Kafka Source ──────────────────────────────────────────────────────────────

raw_kafka_df: DataFrame = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 5000)
    .option("failOnDataLoss", "false")
    .load()
)

# ─── Parse JSON ────────────────────────────────────────────────────────────────

parsed_df: DataFrame = (
    raw_kafka_df
    .select(
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.col("timestamp").alias("kafka_ingest_ts"),
        F.from_json(F.col("value").cast("string"), TRAFFIC_SCHEMA).alias("data"),
    )
    .select("kafka_offset", "kafka_partition", "kafka_ingest_ts", "data.*")
    .withColumn("event_ts", F.to_timestamp("timestamp"))
    .drop("timestamp")
)

# ─── 1. Raw Zone Write (append only, no transforms) ────────────────────────────

raw_query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_RAW)
    .option("path", RAW_PATH)
    .trigger(processingTime="5 seconds")
    .start()
)
print(f"✓ Raw zone streaming to {RAW_PATH}")

# ─── 2. Validation + Enrichment ────────────────────────────────────────────────

def validate_and_enrich(df: DataFrame) -> DataFrame:
    """
    - Drop records with null critical fields or out-of-range values
    - Add a normalised speed_index (0–1)
    - Add pipeline processing timestamp
    - Add event_hour partition column
    """
    valid = (
        df
        .filter(F.col("camera_id").isNotNull())
        .filter(F.col("vehicle_count").between(0, 2000))
        .filter(F.col("average_speed").between(0, 200))
        .filter(F.col("congestion_score").between(0, 100))
        .filter(F.col("event_ts").isNotNull())
        .filter(F.col("latitude").between(-90, 90))
        .filter(F.col("longitude").between(-180, 180))
    )

    enriched = (
        valid
        .withColumn(
            "speed_index",
            F.round(F.col("average_speed") / 120.0, 4),  # normalised 0–1
        )
        .withColumn(
            "congestion_category",
            F.when(F.col("congestion_score") >= 75, "CRITICAL")
             .when(F.col("congestion_score") >= 50, "HIGH")
             .when(F.col("congestion_score") >= 25, "MEDIUM")
             .otherwise("LOW"),
        )
        .withColumn(
            "is_peak_hour",
            F.hour("event_ts").isin([7, 8, 9, 16, 17, 18]),
        )
        .withColumn("pipeline_ts",   F.current_timestamp())
        .withColumn("event_date",    F.to_date("event_ts"))
        .withColumn("event_hour",    F.hour("event_ts"))
    )

    return enriched


clean_df = validate_and_enrich(parsed_df)

clean_query = (
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_CLEAN)
    .option("path", CLEAN_PATH)
    .partitionBy("event_date", "event_hour")
    .trigger(processingTime="5 seconds")
    .start()
)
print(f"✓ Clean zone streaming to {CLEAN_PATH}")

# ─── 3. Windowed Aggregation → Analytics Zone ──────────────────────────────────

aggregated_df = (
    clean_df
    .withWatermark("event_ts", "10 minutes")
    .groupBy(
        F.window("event_ts", "5 minutes", "1 minute"),
        F.col("camera_id"),
        F.col("location"),
    )
    .agg(
        F.avg("vehicle_count").alias("avg_vehicle_count"),
        F.max("vehicle_count").alias("max_vehicle_count"),
        F.min("vehicle_count").alias("min_vehicle_count"),
        F.avg("average_speed").alias("avg_speed"),
        F.min("average_speed").alias("min_speed"),
        F.avg("congestion_score").alias("avg_congestion_score"),
        F.max("congestion_score").alias("max_congestion_score"),
        F.sum(F.col("accident_detected").cast("int")).alias("accident_count"),
        F.count("*").alias("event_count"),
        F.first("congestion_category").alias("congestion_category"),
    )
    .withColumn("window_start",  F.col("window.start"))
    .withColumn("window_end",    F.col("window.end"))
    .withColumn("agg_ts",        F.current_timestamp())
    .drop("window")
)

aggr_query = (
    aggregated_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_AGGR)
    .option("path", ANALYTICS_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)
print(f"✓ Analytics zone streaming to {ANALYTICS_PATH}")

# ─── Wait ──────────────────────────────────────────────────────────────────────

print("SarojFlow streaming pipeline is running. Press Ctrl+C to stop.")
spark.streams.awaitAnyTermination()
