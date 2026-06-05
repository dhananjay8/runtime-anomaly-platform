#!/usr/bin/env python3
"""
ML Anomaly Detection Service

Consumes feature vectors from Kafka, trains Isolation Forest models,
scores incoming vectors for anomalies, and publishes results.

Supports:
- Online scoring per feature vector arrival
- Periodic batch retraining (every 6 hours)
- Model persistence and versioning
- Prometheus metrics endpoint
"""

import json
import os
import signal
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import joblib
import numpy as np
import structlog
from flask import Flask, jsonify, request
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from prometheus_client import Counter, Gauge, Histogram, generate_latest
from sklearn.ensemble import IsolationForest

logger = structlog.get_logger()

# ============================================================================
# Configuration
# ============================================================================

KAFKA_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
FEATURE_VECTORS_TOPIC = os.getenv("FEATURE_VECTORS_TOPIC", "feature-vectors")
ANOMALY_RESULTS_TOPIC = os.getenv("ANOMALY_RESULTS_TOPIC", "anomaly-results")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "ml-group")
MODEL_DIR = os.getenv("MODEL_DIR", "/app/models")
RETRAIN_INTERVAL_HOURS = int(os.getenv("RETRAIN_INTERVAL_HOURS", "6"))
CONTAMINATION = float(os.getenv("CONTAMINATION", "0.05"))
MIN_TRAINING_SAMPLES = int(os.getenv("MIN_TRAINING_SAMPLES", "100"))
FLASK_PORT = int(os.getenv("FLASK_PORT", "8084"))

# Anomaly score thresholds
THRESHOLD_LOW = 0.5
THRESHOLD_MEDIUM = 0.6
THRESHOLD_HIGH = 0.75
THRESHOLD_CRITICAL = 0.9

# Feature names — loaded from shared feature-schema.json (single source of truth)
def _load_feature_schema() -> dict:
    schema_paths = [
        Path(os.getenv("FEATURE_SCHEMA_PATH", "/app/feature-schema.json")),
        Path(__file__).parent / "feature-schema.json",
    ]
    for p in schema_paths:
        if p.exists():
            with open(p) as f:
                return json.load(f)
    logger.warning("feature-schema.json not found, using hardcoded fallback")
    return {
        "version": "1.0",
        "feature_names": [
            "syscall_read_freq", "syscall_write_freq", "syscall_open_freq",
            "syscall_close_freq", "syscall_exec_freq", "syscall_connect_freq",
            "syscall_accept_freq", "syscall_mmap_freq",
            "unique_process_count", "unique_syscall_count", "process_creation_rate",
            "unique_dest_ips", "unique_dest_ports", "network_event_ratio",
            "syscall_entropy", "process_entropy",
            "privileged_event_ratio", "uid_zero_ratio"
        ],
    }

_FEATURE_SCHEMA = _load_feature_schema()
FEATURE_NAMES = _FEATURE_SCHEMA["feature_names"]
SCHEMA_VERSION = _FEATURE_SCHEMA.get("version", "unknown")

# ============================================================================
# Prometheus Metrics
# ============================================================================

EVENTS_SCORED = Counter("ml_events_scored_total", "Total feature vectors scored")
ANOMALIES_DETECTED = Counter("ml_anomalies_detected_total", "Total anomalies detected", ["severity"])
SCORING_LATENCY = Histogram("ml_scoring_latency_seconds", "Time to score a feature vector")
MODEL_VERSION = Gauge("ml_model_version_timestamp", "Timestamp of current model version")
TRAINING_SAMPLES = Gauge("ml_training_samples_count", "Number of samples used in last training")
BUFFER_SIZE = Gauge("ml_training_buffer_size", "Current size of training data buffer")
DRIFT_SCORE = Gauge("ml_drift_score", "Current feature drift score vs baseline")
MODEL_REGISTRY_SIZE = Gauge("ml_model_registry_size", "Number of models in registry")


# ============================================================================
# Anomaly Detection Model
# ============================================================================

class AnomalyDetector:
    """Manages Isolation Forest model lifecycle: training, scoring, persistence."""

    def __init__(self, contamination: float = 0.05, n_estimators: int = 200,
                 max_samples: int = 1024, random_state: int = 42):
        self.contamination = contamination
        self.n_estimators = n_estimators
        self.max_samples = max_samples
        self.random_state = random_state
        self.model: Optional[IsolationForest] = None
        self.model_version: Optional[str] = None
        self.training_buffer: list = []
        self.max_buffer_size = 50000
        self._lock = threading.Lock()
        self._retrain_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="retrain")
        self._retrain_future = None

        # P2.3: per-image baseline means/stds for z-score explanations
        self._image_baselines: dict = {}  # image_name -> {"mean": np.ndarray, "std": np.ndarray, "count": int}

        # P2.4: model registry (version -> metadata)
        self._model_registry: dict = {}
        self._baseline_mean: Optional[np.ndarray] = None
        self._baseline_std: Optional[np.ndarray] = None

        os.makedirs(MODEL_DIR, exist_ok=True)
        self._load_latest_model()
        self._load_model_registry()

    def _load_latest_model(self):
        """Load the most recent persisted model if available."""
        try:
            model_files = [f for f in os.listdir(MODEL_DIR) if f.endswith(".joblib") and f.startswith("model_")]
            if model_files:
                latest = sorted(model_files)[-1]
                path = os.path.join(MODEL_DIR, latest)
                self.model = joblib.load(path)
                self.model_version = latest.replace("model_", "").replace(".joblib", "")
                MODEL_VERSION.set(float(self.model_version) if self.model_version.replace(".", "").isdigit() else 0)
                logger.info("Loaded model", version=self.model_version, path=path)
        except Exception as e:
            logger.warning("No existing model found", error=str(e))

    def _load_model_registry(self):
        """P2.4: Build registry from persisted model files."""
        registry_path = os.path.join(MODEL_DIR, "registry.json")
        if os.path.exists(registry_path):
            try:
                with open(registry_path) as f:
                    self._model_registry = json.load(f)
            except Exception as e:
                logger.warning("Failed to load model registry", error=str(e))
        MODEL_REGISTRY_SIZE.set(len(self._model_registry))

    def _save_model_registry(self):
        registry_path = os.path.join(MODEL_DIR, "registry.json")
        try:
            with open(registry_path, "w") as f:
                json.dump(self._model_registry, f, indent=2, default=str)
        except Exception as e:
            logger.warning("Failed to save model registry", error=str(e))

    def replay_training_buffer(self, feature_rows: list):
        """P2.3: Seed training buffer from persisted feature_vectors on startup."""
        loaded = 0
        for row in feature_rows:
            try:
                vec = np.array(row, dtype=np.float64)
                if len(vec) == len(FEATURE_NAMES):
                    self.training_buffer.append(vec)
                    loaded += 1
            except Exception:
                pass
        if loaded > 0:
            BUFFER_SIZE.set(len(self.training_buffer))
            logger.info("Replayed training buffer from DB", samples=loaded)

    def update_image_baseline(self, image_name: str, features: np.ndarray):
        """P2.3: Online update of per-image running mean/std (Welford's algorithm)."""
        if not image_name:
            return
        if image_name not in self._image_baselines:
            self._image_baselines[image_name] = {
                "mean": features.copy(), "M2": np.zeros_like(features), "count": 1
            }
            return
        b = self._image_baselines[image_name]
        b["count"] += 1
        delta = features - b["mean"]
        b["mean"] += delta / b["count"]
        delta2 = features - b["mean"]
        b["M2"] += delta * delta2

    def _get_image_std(self, image_name: str) -> Optional[np.ndarray]:
        if image_name not in self._image_baselines:
            return None
        b = self._image_baselines[image_name]
        if b["count"] < 2:
            return None
        variance = b["M2"] / (b["count"] - 1)
        return np.sqrt(np.maximum(variance, 1e-8))

    def add_training_sample(self, features: np.ndarray):
        """Add a feature vector to the training buffer."""
        with self._lock:
            self.training_buffer.append(features)
            if len(self.training_buffer) > self.max_buffer_size:
                self.training_buffer = self.training_buffer[-self.max_buffer_size:]
            BUFFER_SIZE.set(len(self.training_buffer))

    def train(self) -> bool:
        """Train a new Isolation Forest model on buffered data."""
        with self._lock:
            if len(self.training_buffer) < MIN_TRAINING_SAMPLES:
                logger.warning("Insufficient training data",
                             samples=len(self.training_buffer),
                             required=MIN_TRAINING_SAMPLES)
                return False

            X = np.array(self.training_buffer)

        logger.info("Training Isolation Forest", samples=X.shape[0], features=X.shape[1])

        X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

        model = IsolationForest(
            contamination=self.contamination,
            n_estimators=self.n_estimators,
            max_samples=min(self.max_samples, X.shape[0]),
            random_state=self.random_state,
            n_jobs=-1,
            warm_start=False,
        )

        model.fit(X)

        version = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        model_path = os.path.join(MODEL_DIR, f"model_{version}.joblib")
        joblib.dump(model, model_path)

        # P2.2: compute and store global baseline mean/std for z-score explanations
        self._baseline_mean = X.mean(axis=0)
        self._baseline_std = np.maximum(X.std(axis=0), 1e-8)

        # P2.4: compute drift score vs previous baseline
        drift = self._compute_drift(X)
        DRIFT_SCORE.set(drift)

        # P2.4: register model metadata
        meta = {
            "version": version,
            "path": model_path,
            "samples": int(X.shape[0]),
            "features": int(X.shape[1]),
            "schema_version": SCHEMA_VERSION,
            "drift_score": round(drift, 6),
            "trained_at": datetime.now(timezone.utc).isoformat(),
        }
        self._model_registry[version] = meta
        self._save_model_registry()
        MODEL_REGISTRY_SIZE.set(len(self._model_registry))

        self.model = model
        self.model_version = version
        MODEL_VERSION.set(float(version))
        TRAINING_SAMPLES.set(X.shape[0])

        logger.info("Model trained and saved", version=version, path=model_path,
                    samples=X.shape[0], drift=drift)
        return True

    def train_async(self):
        """P2.7: Submit training to background thread pool (non-blocking)."""
        if self._retrain_future is not None and not self._retrain_future.done():
            logger.info("Retrain already in progress, skipping")
            return
        self._retrain_future = self._retrain_executor.submit(self.train)
        logger.info("Async retrain submitted")

    def _compute_drift(self, X: np.ndarray) -> float:
        """P2.4: Mean absolute deviation of new training data mean vs baseline mean."""
        if self._baseline_mean is None:
            return 0.0
        new_mean = X.mean(axis=0)
        if self._baseline_std is not None:
            normalized = np.abs(new_mean - self._baseline_mean) / self._baseline_std
        else:
            normalized = np.abs(new_mean - self._baseline_mean)
        return float(normalized.mean())

    def score(self, features: np.ndarray, image_name: Optional[str] = None) -> dict:
        """Score a feature vector and return anomaly result."""
        if self.model is None:
            return {
                "anomaly_score": 0.0,
                "is_anomalous": False,
                "severity": "LOW",
                "model_version": "none",
                "contributing_features": [],
            }

        features_2d = features.reshape(1, -1)
        features_2d = np.nan_to_num(features_2d, nan=0.0, posinf=0.0, neginf=0.0)

        raw_score = -self.model.decision_function(features_2d)[0]
        anomaly_score = max(0.0, min(1.0, (raw_score + 0.5)))

        is_anomalous = anomaly_score > THRESHOLD_LOW
        severity = self._classify_severity(anomaly_score)

        contributing = self._identify_contributing_features(features, image_name=image_name)

        return {
            "anomaly_score": round(anomaly_score, 6),
            "is_anomalous": is_anomalous,
            "severity": severity,
            "model_version": self.model_version or "none",
            "contributing_features": contributing,
        }

    def _classify_severity(self, score: float) -> str:
        """Classify anomaly severity based on score thresholds."""
        if score >= THRESHOLD_CRITICAL:
            return "CRITICAL"
        elif score >= THRESHOLD_HIGH:
            return "HIGH"
        elif score >= THRESHOLD_MEDIUM:
            return "MEDIUM"
        elif score >= THRESHOLD_LOW:
            return "LOW"
        return "NONE"

    def _identify_contributing_features(self, features: np.ndarray,
                                          image_name: Optional[str] = None,
                                          top_k: int = 5) -> list:
        """P2.2: Identify top-k features by z-score deviation from baseline."""
        if len(features) != len(FEATURE_NAMES):
            return []

        # Prefer per-image baseline, fall back to global baseline, then raw magnitude
        mean = None
        std = None
        if image_name and image_name in self._image_baselines:
            b = self._image_baselines[image_name]
            mean = b["mean"]
            std = self._get_image_std(image_name)
        if mean is None and self._baseline_mean is not None:
            mean = self._baseline_mean
            std = self._baseline_std

        if mean is not None and std is not None:
            deviations = np.abs(features - mean) / std
        else:
            deviations = np.abs(features)

        indexed = sorted(enumerate(deviations), key=lambda x: x[1], reverse=True)
        return [FEATURE_NAMES[i] for i, _ in indexed[:top_k]]


# ============================================================================
# Kafka Consumer/Producer
# ============================================================================

class MLPipeline:
    """Kafka consumer/producer pipeline for ML scoring."""

    def __init__(self, detector: AnomalyDetector):
        self.detector = detector
        self.running = False

        self.consumer = KafkaConsumer(
            FEATURE_VECTORS_TOPIC,
            bootstrap_servers=KAFKA_SERVERS,
            group_id=CONSUMER_GROUP,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            max_poll_records=100,
            session_timeout_ms=30000,
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if k else None,
            acks="all",
            retries=3,
            compression_type="lz4",
        )

        logger.info("ML pipeline initialized", consumer_topic=FEATURE_VECTORS_TOPIC,
                     producer_topic=ANOMALY_RESULTS_TOPIC)

    def start(self):
        """Start consuming feature vectors and scoring them."""
        self.running = True
        scored_count = 0

        logger.info("ML pipeline started, consuming feature vectors...")

        while self.running:
            try:
                records = self.consumer.poll(timeout_ms=1000)
                for tp, messages in records.items():
                    for msg in messages:
                        try:
                            self._process_message(msg.value)
                            scored_count += 1
                            if scored_count % 100 == 0:
                                logger.info("Scored vectors", count=scored_count)
                        except Exception as e:
                            logger.error("Failed to process feature vector",
                                       error=str(e), partition=tp.partition, offset=msg.offset)

                self.consumer.commit()

            except Exception as e:
                logger.error("Consumer poll error", error=str(e))
                time.sleep(1)

        self.consumer.close()
        self.producer.flush()
        self.producer.close()
        logger.info("ML pipeline stopped", total_scored=scored_count)

    def stop(self):
        self.running = False

    def _process_message(self, vector_data: dict):
        """Process a single feature vector message."""
        import time as _time

        start = _time.monotonic()

        features = vector_data.get("features")
        if features is None:
            logger.warning("Feature vector missing 'features' field", vector_id=vector_data.get("vector_id"))
            return

        features_array = np.array(features, dtype=np.float64)
        image_name = vector_data.get("image_name")

        # P2.3: update per-image baseline
        self.detector.update_image_baseline(image_name or "", features_array)

        # Add to training buffer
        self.detector.add_training_sample(features_array)

        # Score with per-image context for z-score explanations
        result = self.detector.score(features_array, image_name=image_name)

        elapsed = _time.monotonic() - start
        SCORING_LATENCY.observe(elapsed)
        EVENTS_SCORED.inc()

        # Build anomaly result
        anomaly_result = {
            "result_id": str(uuid.uuid4()),
            "vector_id": vector_data.get("vector_id"),
            "container_id": vector_data.get("container_id"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "anomaly_score": result["anomaly_score"],
            "is_anomalous": result["is_anomalous"],
            "severity": result["severity"],
            "model_version": result["model_version"],
            "contributing_features": result["contributing_features"],
            "description": self._build_description(vector_data, result),
            "window_start": vector_data.get("window_start"),
            "window_end": vector_data.get("window_end"),
        }

        # Publish result
        container_id = vector_data.get("container_id", "unknown")
        self.producer.send(ANOMALY_RESULTS_TOPIC, key=container_id, value=anomaly_result)

        if result["is_anomalous"]:
            ANOMALIES_DETECTED.labels(severity=result["severity"]).inc()
            logger.warning("Anomaly detected",
                         container_id=container_id,
                         score=result["anomaly_score"],
                         severity=result["severity"],
                         features=result["contributing_features"])

    def _build_description(self, vector_data: dict, result: dict) -> str:
        """Build human-readable anomaly description."""
        if not result["is_anomalous"]:
            return "Normal behavior observed"

        parts = [f"Anomalous behavior detected (score: {result['anomaly_score']:.3f})"]

        features = result.get("contributing_features", [])
        if features:
            parts.append(f"Top contributing features: {', '.join(features[:3])}")

        total_events = vector_data.get("total_events", 0)
        parts.append(f"Window contained {total_events} events")

        return ". ".join(parts)


# ============================================================================
# Periodic Retraining
# ============================================================================

class RetrainScheduler:
    """Periodically retrains the Isolation Forest model."""

    def __init__(self, detector: AnomalyDetector, interval_hours: int = 6):
        self.detector = detector
        self.interval_seconds = interval_hours * 3600
        self.running = False
        self._thread = None

    def start(self):
        self.running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("Retrain scheduler started", interval_hours=self.interval_seconds / 3600)

    def stop(self):
        self.running = False

    def _run(self):
        initial_wait = 60
        time.sleep(initial_wait)

        while self.running:
            try:
                logger.info("Submitting async model retrain...")
                self.detector.train_async()
            except Exception as e:
                logger.error("Retrain submission failed", error=str(e))

            time.sleep(self.interval_seconds)


# ============================================================================
# Flask Health/Metrics API
# ============================================================================

app = Flask(__name__)


@app.route("/health")
def health():
    return jsonify({
        "status": "UP",
        "service": "ml-service",
        "model_version": detector.model_version or "none",
        "training_buffer_size": len(detector.training_buffer),
        "model_loaded": detector.model is not None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    })


@app.route("/metrics")
def metrics():
    return generate_latest(), 200, {"Content-Type": "text/plain; charset=utf-8"}


@app.route("/model/info")
def model_info():
    current_meta = detector._model_registry.get(detector.model_version, {})
    return jsonify({
        "model_version": detector.model_version,
        "model_type": "IsolationForest",
        "schema_version": SCHEMA_VERSION,
        "contamination": CONTAMINATION,
        "n_estimators": detector.n_estimators,
        "feature_names": FEATURE_NAMES,
        "feature_count": len(FEATURE_NAMES),
        "training_buffer_size": len(detector.training_buffer),
        "min_training_samples": MIN_TRAINING_SAMPLES,
        "thresholds": {
            "low": THRESHOLD_LOW,
            "medium": THRESHOLD_MEDIUM,
            "high": THRESHOLD_HIGH,
            "critical": THRESHOLD_CRITICAL,
        },
        "lineage": current_meta,
        "drift_score": current_meta.get("drift_score"),
        "registry_size": len(detector._model_registry),
        "image_baselines_tracked": len(detector._image_baselines),
    })


@app.route("/model/registry")
def model_registry():
    return jsonify({
        "registry": detector._model_registry,
        "current_version": detector.model_version,
        "total": len(detector._model_registry),
    })


@app.route("/model/retrain", methods=["POST"])
def trigger_retrain():
    """Manually trigger async model retraining."""
    blocking = request.args.get("blocking", "false").lower() == "true"
    if blocking:
        success = detector.train()
        if success:
            return jsonify({"status": "success", "model_version": detector.model_version})
        return jsonify({"status": "failed", "reason": "insufficient training data"}), 400
    else:
        detector.train_async()
        return jsonify({"status": "submitted", "message": "retrain queued in background"})


@app.route("/score", methods=["POST"])
def score_vector():
    """Score a feature vector via REST (for testing)."""
    data = request.get_json()
    features = data.get("features")
    if not features:
        return jsonify({"error": "missing 'features' field"}), 400

    features_array = np.array(features, dtype=np.float64)
    image_name = data.get("image_name")
    result = detector.score(features_array, image_name=image_name)
    return jsonify(result)


# ============================================================================
# Main Entry Point
# ============================================================================

detector = AnomalyDetector(contamination=CONTAMINATION)


def main():
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )

    logger.info("Starting ML Anomaly Detection Service",
                kafka=KAFKA_SERVERS,
                feature_topic=FEATURE_VECTORS_TOPIC,
                results_topic=ANOMALY_RESULTS_TOPIC)

    # Start retraining scheduler
    scheduler = RetrainScheduler(detector, interval_hours=RETRAIN_INTERVAL_HOURS)
    scheduler.start()

    # Start Kafka pipeline in a background thread
    pipeline = MLPipeline(detector)
    pipeline_thread = threading.Thread(target=pipeline.start, daemon=True)
    pipeline_thread.start()

    def shutdown(sig, frame):
        logger.info("Shutdown signal received")
        pipeline.stop()
        scheduler.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Start Flask API (blocking)
    app.run(host="0.0.0.0", port=FLASK_PORT, debug=False)


if __name__ == "__main__":
    main()
