"""
sarojflow.producer.traffic_producer
────────────────────────────────────
Simulates 12 traffic cameras across a city grid.
Each camera emits a JSON event every 1/EVENTS_PER_SECOND seconds.
Events are published to Kafka topic `traffic.raw.events`.

Usage:
    python -m producer.traffic_producer
    python -m producer.traffic_producer --cameras 12 --rate 10 --duration 3600
"""

from __future__ import annotations

import json
import math
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Dict, List

import click
from kafka import KafkaProducer
from kafka.errors import KafkaError
from loguru import logger

from producer.schema import CongestionLevel, TrafficEvent

# ─── Camera Registry ───────────────────────────────────────────────────────────

CAMERA_REGISTRY = [
    {"camera_id": "CAM_01", "location": "Main St / 5th Ave",
        "lat": 43.6532, "lon": -79.3832},
    {"camera_id": "CAM_02", "location": "Highway 401 Northbound",
        "lat": 43.7615, "lon": -79.4111},
    {"camera_id": "CAM_03", "location": "Downtown Financial Core",
        "lat": 43.6481, "lon": -79.3786},
    {"camera_id": "CAM_04", "location": "Pearson Airport Rd",
        "lat": 43.6777, "lon": -79.6248},
    {"camera_id": "CAM_05", "location": "Bayview Ave / Sheppard",
        "lat": 43.7687, "lon": -79.3768},
    {"camera_id": "CAM_06", "location": "Union Station Approach",
        "lat": 43.6452, "lon": -79.3806},
    {"camera_id": "CAM_07", "location": "Lake Shore Blvd W",
        "lat": 43.6362, "lon": -79.4891},
    {"camera_id": "CAM_08", "location": "Yonge & Eglinton",
        "lat": 43.7072, "lon": -79.3982},
    {"camera_id": "CAM_09", "location": "Don Valley Pkwy S",
        "lat": 43.6629, "lon": -79.3418},
    {"camera_id": "CAM_10", "location": "Gardiner Expressway E",
        "lat": 43.6418, "lon": -79.3521},
    {"camera_id": "CAM_11", "location": "Allen Rd / Lawrence",
        "lat": 43.7232, "lon": -79.4416},
    {"camera_id": "CAM_12", "location": "Scarborough Town Centre",
        "lat": 43.7745, "lon": -79.2569},
]


# ─── Traffic Pattern Simulation ────────────────────────────────────────────────

def rush_hour_multiplier(hour: int) -> float:
    """Gaussian peaks at 08:00 and 17:00 to replicate real rush-hour patterns."""
    am_peak = math.exp(-0.5 * ((hour - 8) / 1.2) ** 2)
    pm_peak = math.exp(-0.5 * ((hour - 17) / 1.5) ** 2)
    night_base = 0.15
    return night_base + 0.85 * max(am_peak, pm_peak)


class CameraSimulator:
    """Maintains per-camera rolling state so events are temporally coherent."""

    BASE_VEHICLE_COUNT: Dict[str, int] = {
        cam["camera_id"]: random.randint(40, 180)
        for cam in CAMERA_REGISTRY
    }

    def __init__(self, meta: dict):
        self.camera_id: str = meta["camera_id"]
        self.location: str = meta["location"]
        self.lat: float = meta["lat"]
        self.lon: float = meta["lon"]
        self._vehicle_count: float = self.BASE_VEHICLE_COUNT[self.camera_id]
        self._speed: float = random.uniform(40, 70)
        self._accident_cooldown: int = 0
        self._spike_cooldown: int = 0

    def _inject_anomaly(self) -> bool:
        """1-in-500 chance of injecting a traffic spike or accident event."""
        roll = random.random()
        if roll < 0.002 and self._spike_cooldown == 0:
            self._vehicle_count *= random.uniform(2.2, 3.5)
            self._speed = max(2, self._speed * 0.3)
            self._spike_cooldown = 30
            logger.warning(f"[ANOMALY] Spike injected on {self.camera_id}")
            return True
        if roll < 0.001 and self._accident_cooldown == 0:
            self._speed = random.uniform(0, 8)
            self._accident_cooldown = 60
            logger.warning(f"[ANOMALY] Accident injected on {self.camera_id}")
            return True
        return False

    def next_event(self) -> TrafficEvent:
        now = datetime.now(timezone.utc)
        multiplier = rush_hour_multiplier(now.hour)

        if self._spike_cooldown > 0:
            self._spike_cooldown -= 1
        if self._accident_cooldown > 0:
            self._accident_cooldown -= 1

        # Drift vehicle count toward realistic range with some noise
        base = self.BASE_VEHICLE_COUNT[self.camera_id] * multiplier
        self._vehicle_count = max(
            0,
            self._vehicle_count * 0.7 + base * 0.3 + random.gauss(0, 8),
        )
        vehicle_count = max(0, int(round(self._vehicle_count)))

        # Speed inversely correlated with congestion
        congestion_raw = min(100, (vehicle_count / 500)
                             * 100 + random.gauss(0, 5))
        self._speed = max(1, 80 - 0.7 * congestion_raw + random.gauss(0, 4))

        accident_detected = self._accident_cooldown > 50
        if accident_detected:
            self._speed = random.uniform(0, 6)
            congestion_raw = min(100, congestion_raw + 30)

        # Classify congestion
        if congestion_raw >= 75:
            level = CongestionLevel.CRITICAL
        elif congestion_raw >= 50:
            level = CongestionLevel.HIGH
        elif congestion_raw >= 25:
            level = CongestionLevel.MEDIUM
        else:
            level = CongestionLevel.LOW

        self._inject_anomaly()

        return TrafficEvent(
            camera_id=self.camera_id,
            location=self.location,
            latitude=self.lat,
            longitude=self.lon,
            vehicle_count=vehicle_count,
            average_speed=round(self._speed, 1),
            congestion_level=level,
            congestion_score=max(0, min(100, round(congestion_raw, 2))),
            accident_detected=accident_detected,
            pedestrian_count=random.randint(0, 80),
            heavy_vehicle_pct=round(random.uniform(5, 25), 1),

        )


# ─── Kafka Producer ────────────────────────────────────────────────────────────

def build_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        compression_type="gzip",
        linger_ms=5,
        batch_size=16384,
    )


def on_send_success(record_metadata):
    logger.debug(
        f"topic={record_metadata.topic} partition={record_metadata.partition} "
        f"offset={record_metadata.offset}"
    )


def on_send_error(exc: KafkaError):
    logger.error(f"Kafka send error: {exc}")


# ─── Main Loop ─────────────────────────────────────────────────────────────────

_running = True


def _handle_signal(sig, frame):
    global _running
    logger.info("Shutdown signal received — stopping producer gracefully.")
    _running = False


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


@click.command()
@click.option("--cameras",  default=12,           help="Number of cameras to simulate.")
@click.option("--rate",     default=10,            help="Events per second (across all cameras).")
@click.option("--duration", default=0,             help="Run duration in seconds. 0 = run forever.")
@click.option("--topic",    default="traffic.raw.events", help="Kafka topic name.")
@click.option("--bootstrap-servers", default=None, help="Kafka bootstrap servers.")
def main(cameras: int, rate: int, duration: int, topic: str, bootstrap_servers: str):
    """SarojFlow — Traffic Camera Kafka Producer"""

    servers = bootstrap_servers or os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    logger.info(f"Connecting to Kafka at {servers}")
    logger.info(
        f"Simulating {cameras} cameras at {rate} events/sec → topic '{topic}'")

    producer = build_kafka_producer(servers)
    simulators: List[CameraSimulator] = [
        CameraSimulator(CAMERA_REGISTRY[i % len(CAMERA_REGISTRY)])
        for i in range(cameras)
    ]

    interval = 1.0 / max(1, rate)
    start_ts = time.monotonic()
    total_sent = 0

    while _running:
        if duration > 0 and (time.monotonic() - start_ts) >= duration:
            logger.info(f"Duration {duration}s reached. Stopping.")
            break

        tick_start = time.monotonic()

        for sim in simulators:
            try:
                event = sim.next_event()
                payload = event.to_kafka_payload()
                (
                    producer.send(topic, key=sim.camera_id, value=payload)
                    .add_callback(on_send_success)
                    .add_errback(on_send_error)
                )
                total_sent += 1
            except Exception as exc:
                logger.error(
                    f"Failed to produce event for {sim.camera_id}: {exc}")

        elapsed = time.monotonic() - tick_start
        sleep_time = max(0.0, interval - elapsed)
        time.sleep(sleep_time)

        if total_sent % 1000 == 0:
            logger.info(f"Total events produced: {total_sent:,}")

    producer.flush(timeout=10)
    producer.close()
    logger.info(f"Producer shut down. Total events sent: {total_sent:,}")


if __name__ == "__main__":
    main()
