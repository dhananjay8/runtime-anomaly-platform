"""
WSGI entry point for gunicorn.
Starts Kafka pipeline and scheduler in background threads before serving.
"""
import threading
import signal
import sys

import structlog

from app import app, detector, RETRAIN_INTERVAL_HOURS
from app import RetrainScheduler, MLPipeline

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer(),
    ]
)

logger = structlog.get_logger()

scheduler = RetrainScheduler(detector, interval_hours=RETRAIN_INTERVAL_HOURS)
scheduler.start()

pipeline = MLPipeline(detector)
pipeline_thread = threading.Thread(target=pipeline.start, daemon=True)
pipeline_thread.start()

logger.info("wsgi: Kafka pipeline and retrain scheduler started")


def _shutdown(sig, frame):
    logger.info("wsgi: shutdown signal received")
    pipeline.stop()
    scheduler.stop()
    sys.exit(0)


signal.signal(signal.SIGINT, _shutdown)
signal.signal(signal.SIGTERM, _shutdown)
