-- ─── SarojFlow Database Schema ───────────────────────────────────────────────
-- Run automatically by Postgres on first container start.

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";

-- ─── Clean Events (mirror of Delta Lake clean zone) ─────────────────────────
CREATE TABLE IF NOT EXISTS clean_events (
    id                  BIGSERIAL PRIMARY KEY,
    event_id            UUID NOT NULL UNIQUE,
    camera_id           VARCHAR(10) NOT NULL,
    location            TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    vehicle_count       INTEGER NOT NULL CHECK (vehicle_count >= 0),
    average_speed       DOUBLE PRECISION NOT NULL CHECK (average_speed >= 0),
    congestion_level    VARCHAR(10) NOT NULL,
    congestion_score    DOUBLE PRECISION NOT NULL CHECK (congestion_score BETWEEN 0 AND 100),
    accident_detected   BOOLEAN DEFAULT FALSE,
    pedestrian_count    INTEGER DEFAULT 0,
    heavy_vehicle_pct   DOUBLE PRECISION,
    speed_index         DOUBLE PRECISION,
    congestion_category VARCHAR(10),
    is_peak_hour        BOOLEAN,
    event_ts            TIMESTAMPTZ NOT NULL,
    event_date          DATE,
    event_hour          SMALLINT,
    pipeline_ts         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_clean_events_camera_ts ON clean_events (camera_id, event_ts DESC);
CREATE INDEX IF NOT EXISTS idx_clean_events_date      ON clean_events (event_date);
CREATE INDEX IF NOT EXISTS idx_clean_events_ts        ON clean_events (event_ts DESC);


-- ─── Anomaly Alerts ─────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    id                      BIGSERIAL PRIMARY KEY,
    alert_id                VARCHAR(100) NOT NULL UNIQUE,
    camera_id               VARCHAR(10) NOT NULL,
    alert_type              VARCHAR(30) NOT NULL,
    severity                VARCHAR(10) NOT NULL,
    anomaly_score           DOUBLE PRECISION NOT NULL,
    message                 TEXT NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    acknowledged            BOOLEAN DEFAULT FALSE,
    acknowledged_at         TIMESTAMPTZ,
    baseline_vehicle_count  DOUBLE PRECISION,
    current_vehicle_count   INTEGER,
    pct_above_baseline      DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_alerts_camera    ON anomaly_alerts (camera_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity  ON anomaly_alerts (severity, acknowledged);
CREATE INDEX IF NOT EXISTS idx_alerts_created   ON anomaly_alerts (created_at DESC);


-- ─── Daily Camera Summaries ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS daily_camera_summary (
    id                  BIGSERIAL PRIMARY KEY,
    summary_date        DATE NOT NULL,
    camera_id           VARCHAR(10) NOT NULL,
    location            TEXT,
    total_events        BIGINT,
    avg_vehicle_count   DOUBLE PRECISION,
    max_vehicle_count   INTEGER,
    avg_speed           DOUBLE PRECISION,
    min_speed           DOUBLE PRECISION,
    total_accidents     INTEGER DEFAULT 0,
    avg_congestion_score DOUBLE PRECISION,
    peak_hour           SMALLINT,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (summary_date, camera_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_summary_date ON daily_camera_summary (summary_date DESC);


-- ─── Pipeline Health Log ────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_health_log (
    id              BIGSERIAL PRIMARY KEY,
    component       VARCHAR(50) NOT NULL,
    status          VARCHAR(20) NOT NULL,
    latency_ms      DOUBLE PRECISION,
    details         TEXT,
    logged_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_health_log_component ON pipeline_health_log (component, logged_at DESC);


-- ─── Seed: camera registry ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS camera_registry (
    camera_id   VARCHAR(10) PRIMARY KEY,
    location    TEXT NOT NULL,
    latitude    DOUBLE PRECISION,
    longitude   DOUBLE PRECISION,
    zone        VARCHAR(5),
    active      BOOLEAN DEFAULT TRUE,
    added_at    TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO camera_registry (camera_id, location, latitude, longitude, zone) VALUES
    ('CAM_01', 'Main St / 5th Ave',       43.6532, -79.3832, 'A'),
    ('CAM_02', 'Highway 401 Northbound',   43.7615, -79.4111, 'B'),
    ('CAM_03', 'Downtown Financial Core',  43.6481, -79.3786, 'A'),
    ('CAM_04', 'Pearson Airport Rd',       43.6777, -79.6248, 'C'),
    ('CAM_05', 'Bayview Ave / Sheppard',   43.7687, -79.3768, 'D'),
    ('CAM_06', 'Union Station Approach',   43.6452, -79.3806, 'A'),
    ('CAM_07', 'Lake Shore Blvd W',        43.6362, -79.4891, 'C'),
    ('CAM_08', 'Yonge & Eglinton',         43.7072, -79.3982, 'B'),
    ('CAM_09', 'Don Valley Pkwy S',        43.6629, -79.3418, 'D'),
    ('CAM_10', 'Gardiner Expressway E',    43.6418, -79.3521, 'A'),
    ('CAM_11', 'Allen Rd / Lawrence',      43.7232, -79.4416, 'E'),
    ('CAM_12', 'Scarborough Town Centre',  43.7745, -79.2569, 'E')
ON CONFLICT (camera_id) DO NOTHING;
