# SarojFlow — Distributed Real-Time Smart City Data Platform

" SarojFlow is a real-time data engineering project I built to simulate smart city traffic analytics using Kafka, PySpark, Airflow, and Delta Lake. It streams live traffic and camera data, processes and cleans it using Spark Structured Streaming, stores the processed data in a lakehouse setup, and shows real-time insights through dashboards with AI-based anomaly detection.

## Tech Stack

| Layer             | Technology                                     |
| ----------------- | ---------------------------------------------- |
| Ingestion         | Apache Kafka 7.5, Confluent Platform           |
| Stream Processing | Apache Spark 3.5, PySpark Structured Streaming |
| Storage           | Delta Lake 3.0, Parquet, PostgreSQL 15         |
| Orchestration     | Apache Airflow 2.8                             |
| AI / ML           | scikit-learn IsolationForest, joblib           |
| API               | FastAPI 0.109, Uvicorn, Pydantic v2            |
| Dashboard         | HTML/CSS/JS, Chart.js                          |
| Containerisation  | Docker, Docker Compose                         |
| Schema Registry   | Confluent Schema Registry                      |
| Kafka UI          | Provectus Kafka UI                             |

## How To Run

### Prerequisites

- Docker Desktop ≥ 24 with Compose V2
- 8 GB RAM minimum (Spark + Kafka)
- Ports free: 3000, 4040, 5432, 8000, 8080, 8081, 8082, 9092

### 1. Clone and configure

git clone https://github.com/yourname/sarojflow.git
cd sarojflow
cp .env.example .env

### 3. Verify everything is running

bash
docker compose ps

All services should show `healthy` or `running`.

### 4. Open the interfaces

| Interface        | URL                        | Credentials   |
| ---------------- | -------------------------- | ------------- |
| **Dashboard**    | http://localhost:3000      | —             |
| **FastAPI docs** | http://localhost:8000/docs | —             |
| **Kafka UI**     | http://localhost:8080      | —             |
| **Airflow**      | http://localhost:8082      | admin / admin |
| **Spark UI**     | http://localhost:4040      | —             |

### 5. Run anomaly detection manually

```bash
docker compose exec spark python -m spark.anomaly_detection --window-hours 1 --dry-run
```

### 6. Trigger the Airflow DAG manually

Open http://localhost:8082 → DAGs → `sarojflow_daily_aggregation` → Trigger DAG.

### 7. Query the API

# Live camera metrics

curl http://localhost:8000/v1/metrics/live | python -m json.tool

# Platform summary KPIs

curl http://localhost:8000/v1/metrics/summary | python -m json.tool

# Recent anomaly alerts

curl http://localhost:8000/v1/alerts?severity=CRITICAL | python -m json.tool

# Pipeline health

curl http://localhost:8000/v1/pipeline/health | python -m json.tool

### 8. Run producer standalone (outside Docker)

python -m venv .venv && source .venv/bin/activate
pip install kafka-python pydantic loguru click python-dotenv
python -m producer.traffic_producer --cameras 12 --rate 10

### 9. Stop

docker compose down

---

## Data Schema

Every camera event:

```json
{
  "event_id": "uuid",
  "camera_id": "CAM_04",
  "timestamp": "2025-01-15T09:14:22Z",
  "location": "Pearson Airport Rd",
  "latitude": 43.6777,
  "longitude": -79.6248,
  "vehicle_count": 441,
  "average_speed": 22.3,
  "congestion_level": "HIGH",
  "congestion_score": 81.4,
  "accident_detected": false,
  "pedestrian_count": 12,
  "heavy_vehicle_pct": 18.2,
  "schema_version": "1.0"
}
```

---

## Delta Lake Zones

| Zone      | Path                    | Format  | Partitioning              |
| --------- | ----------------------- | ------- | ------------------------- |
| Raw       | `data/delta/raw/`       | Parquet | none                      |
| Clean     | `data/delta/clean/`     | Delta   | `event_date / event_hour` |
| Analytics | `data/delta/analytics/` | Delta   | `window_start`            |

---

## Anomaly Detection

Uses **scikit-learn IsolationForest** trained on 48h rolling window.

Alert types:

- `TRAFFIC_SPIKE` — vehicle count ≥ 200% above per-camera baseline
- `ACCIDENT` — `accident_detected=true` + speed < 10 km/h
- `LOW_SPEED` — average speed < 10 km/h
- `UNUSUAL_COUNT` — anomaly score ≥ 0.85

Alerts written to:

- Kafka topic `traffic.anomalies`
- Kafka topic `traffic.alerts`
- PostgreSQL `anomaly_alerts` table
- Dashboard alert panel (via FastAPI)

## Environment Variables

| Variable                  | Default          | Description                      |
| ------------------------- | ---------------- | -------------------------------- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker address             |
| `EVENTS_PER_SECOND`       | `10`             | Producer throughput              |
| `NUM_CAMERAS`             | `12`             | Simulated cameras                |
| `POSTGRES_PASSWORD`       | `sarojflow123`   | DB password                      |
| `ANOMALY_THRESHOLD`       | `0.85`           | IsolationForest cutoff           |
| `CONGESTION_SPIKE_PCT`    | `200`            | % above baseline for spike alert |
| `DELTA_LAKE_PATH`         | `./data/delta`   | Delta Lake root path             |

=======

## License

This project is licensed under the MIT License - see the LICENSE file for details. This is subjected to COPYRIGHT under Saroj Neupane.

## Developer

- Saroj Neupane
- Data Engineer
- C
