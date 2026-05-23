"""
sarojflow.spark.anomaly_detection
──────────────────────────────────
Isolation Forest–based anomaly detector.

Reads the `clean` Delta Lake zone, computes per-camera baselines,
fits an IsolationForest, and writes anomaly alerts to:
  - Kafka topic `traffic.anomalies`
  - Postgres table `anomaly_alerts`

Run as a scheduled batch job (via Airflow) or as a micro-batch
alongside the main streaming pipeline.

Usage:
    python -m spark.anomaly_detection
    python -m spark.anomaly_detection --window-hours 48 --threshold 0.85
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import click
import joblib
import numpy as np
import pandas as pd
import psycopg2
from kafka import KafkaProducer
from loguru import logger
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ─── Config ────────────────────────────────────────────────────────────────────

POSTGRES_URL    = os.getenv("POSTGRES_URL", "postgresql://sarojflow:sarojflow123@localhost:5432/sarojflow")
KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ANOMALY_TOPIC   = os.getenv("KAFKA_TOPIC_ANOMALIES", "traffic.anomalies")
ALERT_TOPIC     = os.getenv("KAFKA_TOPIC_ALERTS",    "traffic.alerts")
MODEL_PATH      = os.getenv("MODEL_PATH", "./ml/models/isolation_forest.pkl")
SCALER_PATH     = os.getenv("SCALER_PATH", "./ml/models/scaler.pkl")
THRESHOLD       = float(os.getenv("ANOMALY_THRESHOLD", "0.85"))
SPIKE_PCT       = float(os.getenv("CONGESTION_SPIKE_PCT", "200"))
MIN_SPEED_ALERT = float(os.getenv("MIN_SPEED_ALERT", "10"))
CLEAN_PATH      = os.getenv("DELTA_LAKE_PATH", "./data/delta") + "/clean"

FEATURE_COLS = [
    "vehicle_count",
    "average_speed",
    "congestion_score",
    "pedestrian_count",
    "heavy_vehicle_pct",
    "event_hour",
]


# ─── Database helpers ──────────────────────────────────────────────────────────

def get_pg_conn():
    return psycopg2.connect(POSTGRES_URL)


def save_alert(conn, alert: dict):
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO anomaly_alerts (
                alert_id, camera_id, alert_type, severity,
                anomaly_score, message, created_at,
                baseline_vehicle_count, current_vehicle_count, pct_above_baseline
            ) VALUES (
                %(alert_id)s, %(camera_id)s, %(alert_type)s, %(severity)s,
                %(anomaly_score)s, %(message)s, %(created_at)s,
                %(baseline_vehicle_count)s, %(current_vehicle_count)s,
                %(pct_above_baseline)s
            )
            ON CONFLICT (alert_id) DO NOTHING;
            """,
            alert,
        )
    conn.commit()


# ─── Kafka helpers ─────────────────────────────────────────────────────────────

def build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        acks="all",
    )


# ─── Model Training ────────────────────────────────────────────────────────────

def load_clean_data(window_hours: int) -> pd.DataFrame:
    """
    Load recent records from the clean Delta Lake zone using pandas+pyarrow.
    Falls back to PostgreSQL query if Delta files aren't available locally.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=window_hours)

    try:
        import pyarrow.dataset as ds

        dataset = ds.dataset(CLEAN_PATH, format="parquet")
        table = dataset.to_table(
            filter=ds.field("event_ts") >= cutoff,
            columns=FEATURE_COLS + ["camera_id", "event_ts", "event_hour"],
        )
        df = table.to_pandas()
        logger.info(f"Loaded {len(df):,} records from Delta Lake (last {window_hours}h)")
        return df

    except Exception as exc:
        logger.warning(f"Delta Lake read failed ({exc}), falling back to Postgres")
        conn = get_pg_conn()
        df = pd.read_sql(
            f"""
            SELECT {', '.join(FEATURE_COLS + ['camera_id', 'event_ts'])}
            FROM clean_events
            WHERE event_ts >= %s
            ORDER BY event_ts DESC
            LIMIT 500000
            """,
            conn,
            params=(cutoff,),
        )
        conn.close()
        return df


def train_model(df: pd.DataFrame) -> tuple[IsolationForest, StandardScaler, pd.DataFrame]:
    """Fit IsolationForest per-camera on cleaned features."""
    df = df.dropna(subset=FEATURE_COLS)

    scaler = StandardScaler()
    X = scaler.fit_transform(df[FEATURE_COLS].astype(float))

    model = IsolationForest(
        n_estimators=200,
        max_samples="auto",
        contamination=0.03,      # expect ~3% anomalous events
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X)

    # Per-camera baselines (used in alert messages)
    baselines = (
        df.groupby("camera_id")["vehicle_count"]
        .mean()
        .reset_index()
        .rename(columns={"vehicle_count": "baseline_vehicle_count"})
    )

    logger.info(
        f"IsolationForest trained: {len(df):,} samples, "
        f"{model.n_estimators} trees, contamination=0.03"
    )
    return model, scaler, baselines


def save_model(model: IsolationForest, scaler: StandardScaler):
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    joblib.dump(model, MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)
    logger.info(f"Model saved to {MODEL_PATH}")


def load_model() -> tuple[Optional[IsolationForest], Optional[StandardScaler]]:
    if os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH):
        return joblib.load(MODEL_PATH), joblib.load(SCALER_PATH)
    return None, None


# ─── Inference ─────────────────────────────────────────────────────────────────

def score_events(
    df: pd.DataFrame,
    model: IsolationForest,
    scaler: StandardScaler,
    baselines: pd.DataFrame,
) -> pd.DataFrame:
    """Return rows where IsolationForest predicts anomaly (score <= threshold)."""
    df = df.dropna(subset=FEATURE_COLS).copy()
    X = scaler.transform(df[FEATURE_COLS].astype(float))

    # IsolationForest: higher score_samples = more normal
    raw_scores = model.score_samples(X)                          # negative, range ~[-0.7, 0]
    normalised = (raw_scores - raw_scores.min()) / (raw_scores.max() - raw_scores.min() + 1e-9)
    df["anomaly_raw"] = raw_scores
    df["anomaly_score"] = 1 - normalised                        # flip: 1 = very anomalous

    anomalies = df[df["anomaly_score"] >= THRESHOLD].copy()
    anomalies = anomalies.merge(baselines, on="camera_id", how="left")
    return anomalies


def classify_alert(row: pd.Series) -> dict:
    """Determine alert type and severity from anomalous row."""
    cam = row["camera_id"]
    vc  = int(row.get("vehicle_count", 0))
    spd = float(row.get("average_speed", 0))
    base = float(row.get("baseline_vehicle_count", 100))
    pct_above = round((vc - base) / (base + 1) * 100, 1)

    if spd < MIN_SPEED_ALERT and row.get("accident_detected", False):
        alert_type = "ACCIDENT"
        severity   = "CRITICAL"
        msg = (
            f"Accident pattern detected on {cam}. "
            f"Speed dropped to {spd} km/h with accident_detected=True."
        )
    elif pct_above >= SPIKE_PCT:
        alert_type = "TRAFFIC_SPIKE"
        severity   = "CRITICAL"
        msg = (
            f"Anomaly detected: {cam} has {pct_above}% higher vehicle count than normal "
            f"({vc} vs baseline {base:.0f})."
        )
    elif spd < MIN_SPEED_ALERT:
        alert_type = "LOW_SPEED"
        severity   = "WARNING"
        msg = f"{cam} reporting critically low speed: {spd} km/h. Possible blockage."
    else:
        alert_type = "UNUSUAL_COUNT"
        severity   = "WARNING"
        msg = (
            f"Unusual vehicle count on {cam}: {vc} "
            f"(anomaly score {row['anomaly_score']:.3f})."
        )

    return {
        "alert_id": f"{cam}_{int(time.time())}_{alert_type}",
        "camera_id": cam,
        "alert_type": alert_type,
        "severity": severity,
        "anomaly_score": round(float(row["anomaly_score"]), 4),
        "message": msg,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "baseline_vehicle_count": round(base, 2),
        "current_vehicle_count": vc,
        "pct_above_baseline": pct_above,
    }


# ─── CLI ───────────────────────────────────────────────────────────────────────

@click.command()
@click.option("--window-hours", default=48, help="Hours of data to train on.")
@click.option("--threshold",    default=THRESHOLD, help="Anomaly score threshold (0–1).")
@click.option("--retrain",      is_flag=True,       help="Force model retraining.")
@click.option("--dry-run",      is_flag=True,       help="Score but don't write alerts.")
def main(window_hours: int, threshold: float, retrain: bool, dry_run: bool):
    """SarojFlow — Anomaly Detection & Alert Emitter"""

    logger.info("Loading clean event data...")
    df = load_clean_data(window_hours)

    if df.empty:
        logger.warning("No data found — nothing to score.")
        return

    model, scaler = load_model()
    if model is None or retrain:
        logger.info("Training new IsolationForest model...")
        model, scaler, baselines = train_model(df)
        if not dry_run:
            save_model(model, scaler)
    else:
        logger.info("Loaded existing model from disk.")
        baselines = (
            df.groupby("camera_id")["vehicle_count"]
            .mean()
            .reset_index()
            .rename(columns={"vehicle_count": "baseline_vehicle_count"})
        )

    logger.info(f"Scoring {len(df):,} events (threshold={threshold})...")
    anomalies = score_events(df, model, scaler, baselines)
    logger.info(f"Found {len(anomalies)} anomalous events.")

    if anomalies.empty or dry_run:
        if dry_run:
            logger.info("[DRY RUN] No alerts written.")
        return

    producer = build_producer()
    conn = get_pg_conn()
    emitted = 0

    for _, row in anomalies.iterrows():
        alert = classify_alert(row)
        logger.warning(f"ALERT [{alert['severity']}] {alert['message']}")
        producer.send(ANOMALY_TOPIC, key=alert["camera_id"], value=alert)
        producer.send(ALERT_TOPIC,   key=alert["camera_id"], value=alert)
        save_alert(conn, alert)
        emitted += 1

    producer.flush()
    producer.close()
    conn.close()
    logger.info(f"Emitted {emitted} alerts to Kafka + Postgres.")


if __name__ == "__main__":
    main()
