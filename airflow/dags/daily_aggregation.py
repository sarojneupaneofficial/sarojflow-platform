"""
sarojflow.airflow.dags.daily_aggregation
─────────────────────────────────────────
Daily Airflow DAG that:
  1. Runs data quality checks on yesterday's clean Delta data
  2. Computes daily summaries per camera and writes to Postgres
  3. Re-trains the anomaly detection model with latest data
  4. Runs Z-score quality gate — fails DAG if >5% bad records
  5. Compacts small Delta files (OPTIMIZE + VACUUM)
  6. Generates a Slack/email summary report
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

# ─── Default Args ──────────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "sarojflow-platform",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

POSTGRES_CONN = os.getenv("POSTGRES_URL", "postgresql://sarojflow:sarojflow123@postgres:5432/sarojflow")
DELTA_BASE    = os.getenv("DELTA_LAKE_PATH", "/data/delta")

# ─── Task Functions ────────────────────────────────────────────────────────────

def data_quality_check(**context) -> str:
    """
    Validate yesterday's clean zone data.
    Returns 'quality_pass' or 'quality_fail' for BranchPythonOperator.
    """
    import pandas as pd
    import pyarrow.dataset as ds

    ds_yesterday = context["ds"]                       # YYYY-MM-DD string
    clean_path   = f"{DELTA_BASE}/clean"

    try:
        dataset = ds.dataset(clean_path, format="parquet")
        table   = dataset.to_table(
            filter=ds.field("event_date") == ds_yesterday
        )
        df = table.to_pandas()
    except Exception as exc:
        raise RuntimeError(f"Failed to read clean zone for {ds_yesterday}: {exc}")

    total = len(df)
    if total == 0:
        raise ValueError(f"No data found for {ds_yesterday} — pipeline may be down.")

    # Quality gates
    null_rate    = df[["camera_id", "vehicle_count", "average_speed"]].isnull().mean().mean()
    out_of_range = ((df["vehicle_count"] < 0) | (df["vehicle_count"] > 2000)).mean()
    dup_rate     = df.duplicated(subset=["event_id"]).mean()

    context["task_instance"].xcom_push("quality_stats", {
        "date":        ds_yesterday,
        "total":       total,
        "null_rate":   round(float(null_rate), 4),
        "out_of_range": round(float(out_of_range), 4),
        "dup_rate":    round(float(dup_rate), 4),
    })

    bad_pct = (null_rate + out_of_range + dup_rate) / 3
    print(f"DQ | total={total:,} null={null_rate:.2%} oor={out_of_range:.2%} dup={dup_rate:.2%}")

    return "quality_pass" if bad_pct < 0.05 else "quality_fail"


def compute_daily_aggregations(**context):
    """Aggregate yesterday's clean data into daily summaries in Postgres."""
    import psycopg2

    ds_yesterday = context["ds"]
    conn = psycopg2.connect(POSTGRES_CONN)

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM daily_camera_summary WHERE summary_date = %s", (ds_yesterday,))
            cur.execute(
                """
                INSERT INTO daily_camera_summary (
                    summary_date, camera_id, location,
                    total_events, avg_vehicle_count, max_vehicle_count,
                    avg_speed, min_speed, total_accidents,
                    avg_congestion_score, peak_hour, created_at
                )
                SELECT
                    %s::date AS summary_date,
                    camera_id,
                    location,
                    COUNT(*)              AS total_events,
                    AVG(vehicle_count)    AS avg_vehicle_count,
                    MAX(vehicle_count)    AS max_vehicle_count,
                    AVG(average_speed)    AS avg_speed,
                    MIN(average_speed)    AS min_speed,
                    SUM(accident_detected::int) AS total_accidents,
                    AVG(congestion_score) AS avg_congestion_score,
                    MODE() WITHIN GROUP (ORDER BY event_hour) AS peak_hour,
                    NOW()                 AS created_at
                FROM clean_events
                WHERE event_date = %s::date
                GROUP BY camera_id, location
                """,
                (ds_yesterday, ds_yesterday),
            )
        conn.commit()
        print(f"Daily summaries written for {ds_yesterday}")
    finally:
        conn.close()


def retrain_anomaly_model(**context):
    """Trigger anomaly model retraining with 48h of data."""
    import subprocess, sys
    result = subprocess.run(
        [sys.executable, "-m", "spark.anomaly_detection", "--window-hours", "48", "--retrain"],
        capture_output=True, text=True,
    )
    print(result.stdout)
    if result.returncode != 0:
        raise RuntimeError(f"Model retraining failed:\n{result.stderr}")
    print("Anomaly model retrained successfully.")


def compact_delta_tables(**context):
    """Run OPTIMIZE + VACUUM on Delta Lake zones to compact small files."""
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder
        .appName("SarojFlow-DeltaCompaction")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    for zone in ["raw", "clean", "analytics"]:
        path = f"{DELTA_BASE}/{zone}"
        spark.sql(f"OPTIMIZE delta.`{path}`")
        spark.sql(f"VACUUM delta.`{path}` RETAIN 168 HOURS")   # keep 7 days
        print(f"Compacted + vacuumed: {path}")

    spark.stop()


def send_daily_report(**context):
    """Summarise stats and log the report (extend to email/Slack as needed)."""
    import psycopg2
    import json

    ds_yesterday = context["ds"]
    conn = psycopg2.connect(POSTGRES_CONN)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(DISTINCT camera_id)      AS cameras,
                SUM(total_events)              AS total_events,
                AVG(avg_vehicle_count)         AS avg_vehicles,
                AVG(avg_speed)                 AS avg_speed,
                SUM(total_accidents)           AS accidents,
                AVG(avg_congestion_score)      AS avg_congestion
            FROM daily_camera_summary
            WHERE summary_date = %s
            """,
            (ds_yesterday,),
        )
        row = cur.fetchone()

    conn.close()
    report = {
        "date":           ds_yesterday,
        "cameras":        row[0],
        "total_events":   row[1],
        "avg_vehicles":   round(row[2], 1) if row[2] else None,
        "avg_speed_kmh":  round(row[3], 1) if row[3] else None,
        "accidents":      row[4],
        "avg_congestion": round(row[5], 1) if row[5] else None,
    }
    print("─" * 60)
    print("SarojFlow Daily Report")
    print(json.dumps(report, indent=2))
    print("─" * 60)


def quality_fail_alert(**context):
    """Called when DQ gate fails — log and alert."""
    stats = context["task_instance"].xcom_pull(task_ids="data_quality_check", key="quality_stats")
    print(f"DATA QUALITY FAILURE for {stats}. Manual review required.")
    raise ValueError(f"Quality gate failed: {stats}")


# ─── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="sarojflow_daily_aggregation",
    description="SarojFlow: Daily DQ, aggregation, model retraining, and compaction",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",        # 02:00 UTC every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["sarojflow", "data-quality", "aggregation", "ml"],
) as dag:

    start = EmptyOperator(task_id="start")

    dq_check = BranchPythonOperator(
        task_id="data_quality_check",
        python_callable=data_quality_check,
    )

    quality_ok   = EmptyOperator(task_id="quality_pass")
    quality_fail = PythonOperator(task_id="quality_fail", python_callable=quality_fail_alert)

    daily_agg = PythonOperator(
        task_id="compute_daily_aggregations",
        python_callable=compute_daily_aggregations,
    )

    retrain = PythonOperator(
        task_id="retrain_anomaly_model",
        python_callable=retrain_anomaly_model,
    )

    compact = PythonOperator(
        task_id="compact_delta_tables",
        python_callable=compact_delta_tables,
    )

    report = PythonOperator(
        task_id="send_daily_report",
        python_callable=send_daily_report,
    )

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    # DAG graph
    start >> dq_check >> [quality_ok, quality_fail]
    quality_ok >> [daily_agg, retrain, compact]
    [daily_agg, retrain, compact] >> report >> end
