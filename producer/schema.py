"""
sarojflow.producer.schema
─────────────────────────
Pydantic schema for a single traffic camera event.
This is the canonical contract between producers and consumers.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class CongestionLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class TrafficEvent(BaseModel):
    """
    One observation from a single traffic camera.
    Produced every ~1–5 s per camera; written to Kafka topic traffic.raw.events.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    camera_id: str = Field(..., pattern=r"^CAM_\d{2}$")
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    location: str
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)

    # Traffic metrics
    vehicle_count: int = Field(..., ge=0, le=2000)
    average_speed: float = Field(..., ge=0, le=200)          # km/h
    congestion_level: CongestionLevel
    congestion_score: float = Field(..., ge=0, le=100)        # 0=free, 100=gridlock

    # Derived / enriched fields
    accident_detected: bool = False
    pedestrian_count: int = Field(default=0, ge=0)
    heavy_vehicle_pct: float = Field(default=0.0, ge=0, le=100)

    # Pipeline metadata
    producer_version: str = "2.4.1"
    schema_version: str = "1.0"

    @field_validator("congestion_score")
    @classmethod
    def score_must_match_level(cls, v: float, info) -> float:  # noqa: N805
        level = info.data.get("congestion_level")
        if level == CongestionLevel.CRITICAL and v < 75:
            raise ValueError("CRITICAL congestion_level requires score >= 75")
        return round(v, 2)

    def to_kafka_payload(self) -> dict:
        """Serialise to dict for JSON Kafka message."""
        return self.model_dump(mode="json")

    @classmethod
    def from_kafka_payload(cls, data: dict) -> "TrafficEvent":
        return cls(**data)


class AnomalyAlert(BaseModel):
    """Enriched alert emitted to traffic.anomalies topic."""

    alert_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    camera_id: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    alert_type: str          # TRAFFIC_SPIKE | ACCIDENT | LOW_SPEED | UNUSUAL_COUNT
    severity: str            # WARNING | CRITICAL
    anomaly_score: float     # 0–1 from IsolationForest
    message: str
    baseline_vehicle_count: Optional[float] = None
    current_vehicle_count: Optional[int] = None
    pct_above_baseline: Optional[float] = None

    def to_kafka_payload(self) -> dict:
        return self.model_dump(mode="json")
