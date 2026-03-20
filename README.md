# eBPF Runtime Fingerprinting and Anomaly Detection Platform

A production-grade distributed system that detects anomalous runtime behavior of containerized workloads using eBPF telemetry and machine learning (Isolation Forest).

## Architecture

```
eBPF Collector → Kafka[runtime-events] → Ingestion Service → Kafka[processed-events]
  → Feature Engineering → Kafka[feature-vectors] → ML Service → Kafka[anomaly-results]
  → Detection API → REST Clients
```

**Services:**

| Service | Technology | Port | Description |
|---------|-----------|------|-------------|
| eBPF Collector | Python/BCC | N/A | Kernel telemetry capture (simulation mode on Mac) |
| Ingestion Service | Spring Boot | 8081 | Event validation, normalization, enrichment |
| Feature Engineering | Spring Boot | 8082 | Sliding window feature vector construction |
| ML Service | Python/sklearn | 8084 | Isolation Forest training and scoring |
| Detection API | Spring Boot | 8083 | REST API for anomalies and container profiles |
| Kafka | Confluent | 9092 | Event streaming |
| Oracle XE | Oracle 21c | 1521 | Persistent storage |
| Kafka UI | Provectus | 9080 | Kafka monitoring dashboard |

## Prerequisites

- **Java 17** (OpenJDK or Oracle JDK)
- **Podman** (NOT Docker) + podman-compose
- **Python 3.11+** (for eBPF collector and ML service)
- **Gradle 8.6** (wrapper included)

## Quick Start

### 1. Start Infrastructure (Kafka + Oracle XE)

```bash
cd /Users/dhapatil/workspace/cwp/runtime-anomaly-platform
podman-compose -f infra/podman-compose.yaml up -d zookeeper kafka oracle-xe kafka-init
```

Wait for Oracle XE to be healthy (~2 minutes):
```bash
podman ps --filter name=rap- --format "{{.Names}} {{.Status}}"
```

### 2. Build Java Services

```bash
./gradlew clean build -x test
```

### 3. Start Spring Boot Services

**Terminal 1 — Ingestion Service:**
```bash
java -jar ingestion-service/build/libs/ingestion-service-1.0.0-SNAPSHOT.jar
```

**Terminal 2 — Feature Engineering Service:**
```bash
java -jar feature-service/build/libs/feature-service-1.0.0-SNAPSHOT.jar
```

**Terminal 3 — Detection API:**
```bash
java -jar detection-api/build/libs/detection-api-1.0.0-SNAPSHOT.jar
```

### 4. Start Python Services

**Terminal 4 — ML Service:**
```bash
cd ml-service
pip install -r requirements.txt
python app.py
```

**Terminal 5 — eBPF Collector (Simulation Mode):**
```bash
cd ebpf-collector
pip install -r requirements.txt
python collector.py --mode simulate --eps 50 --anomaly-rate 0.05
```

### 5. Verify

```bash
# Health check
curl http://localhost:8083/api/v1/health | python3 -m json.tool

# ML service health
curl http://localhost:8084/health | python3 -m json.tool

# List anomalies (after ~2 minutes of data collection)
curl "http://localhost:8083/api/v1/anomalies?size=5" | python3 -m json.tool

# Anomaly statistics
curl "http://localhost:8083/api/v1/anomalies/stats?hours=1" | python3 -m json.tool

# Container profiles
curl http://localhost:8083/api/v1/containers | python3 -m json.tool

# Kafka UI
open http://localhost:9080
```

### Alternative: Start Everything with Podman Compose

```bash
podman-compose -f infra/podman-compose.yaml up -d
```

This starts all infrastructure AND the eBPF collector and ML service as containers.
Spring Boot services still need to be started separately (or add Dockerfiles for them).

## API Documentation

OpenAPI/Swagger UI available at: `http://localhost:8083/swagger-ui.html`

OpenAPI spec: `docs/api/openapi.yaml`

### Key Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Platform health (DB + Kafka) |
| GET | `/api/v1/anomalies` | Paginated anomaly list with filters |
| GET | `/api/v1/anomalies/{id}` | Single anomaly detail |
| GET | `/api/v1/anomalies/stats` | Anomaly statistics |
| GET | `/api/v1/containers` | Monitored container list |
| GET | `/api/v1/containers/{id}/profile` | Container behavioral fingerprint |

### Query Parameters

- `page` — Page number (0-indexed)
- `size` — Page size (default 20)
- `severity` — Filter: LOW, MEDIUM, HIGH, CRITICAL
- `containerId` — Filter by container ID
- `from`, `to` — ISO-8601 time range
- `hours` — Time window for stats (default 24)

## Project Structure

```
runtime-anomaly-platform/
├── build.gradle.kts                # Root Gradle (Kotlin DSL)
├── settings.gradle.kts             # Multi-module
├── common-lib/                     # Shared DTOs, events, utilities
├── ingestion-service/              # Kafka consumer → Oracle DB → Kafka
├── feature-service/                # Feature vector engineering
├── detection-api/                  # REST API + Kafka consumer
├── ebpf-collector/                 # Python/BCC agent
├── ml-service/                     # Python Isolation Forest
├── infra/
│   ├── podman-compose.yaml         # Full infrastructure stack
│   └── oracle/init/                # DB schema auto-init
├── db/migration/                   # SQL migration scripts
├── docs/
│   ├── research-paper.md           # IEEE-format research paper
│   ├── diagrams/                   # Draw.io architecture diagrams
│   └── api/openapi.yaml            # OpenAPI 3.0 spec
└── README.md
```

## Database Schema

| Table | Purpose | Key Indexes |
|-------|---------|-------------|
| `runtime_events` | Raw eBPF telemetry | container_id, timestamp |
| `processed_events` | Normalized events | container_id+timestamp, syscall_category |
| `feature_vectors` | Time-windowed features | container_id+window_start |
| `anomaly_results` | ML scoring output | container_id+detected_at, severity |
| `container_profiles` | Behavioral baselines | risk_level, last_seen |
| `alert_history` | Triggered alerts | severity, created_at |

## Kafka Topics

| Topic | Partitions | Retention | Consumer Group |
|-------|-----------|-----------|----------------|
| runtime-events | 12 | 7 days | ingestion-group |
| processed-events | 12 | 7 days | feature-group |
| feature-vectors | 6 | 3 days | ml-group |
| anomaly-results | 6 | 14 days | detection-group |
| *.DLT | 3 | 30 days | dlq-processor-group |

## ML Model

- **Algorithm:** Isolation Forest (scikit-learn)
- **Features:** 18-dimensional behavioral vectors
- **Training:** Batch retrain every 6 hours
- **Scoring:** Online per feature vector (<1ms)
- **Severity thresholds:** LOW (0.5), MEDIUM (0.6), HIGH (0.75), CRITICAL (0.9)

## Diagrams

Open `.drawio` files in [draw.io](https://app.diagrams.net/) or VS Code with the Draw.io Integration extension:

- `docs/diagrams/architecture.drawio` — System architecture overview
- `docs/diagrams/data-flow.drawio` — End-to-end data flow pipeline
- `docs/diagrams/ml-pipeline.drawio` — ML scoring and training pipeline

## Troubleshooting

### Oracle XE not starting
```bash
podman logs rap-oracle-xe
# Wait for "DATABASE IS READY TO USE" message (~2 min on first start)
```

### Kafka topics not created
```bash
podman exec rap-kafka kafka-topics --list --bootstrap-server localhost:9092
```

### ML service not detecting anomalies
```bash
# Check training buffer size (needs >= 100 samples)
curl http://localhost:8084/model/info | python3 -m json.tool

# Manually trigger retraining
curl -X POST http://localhost:8084/model/retrain
```

### Check service logs
```bash
# Spring Boot services log to console and logs/ directory
tail -f logs/ingestion-service.log

# ML service
podman logs rap-ml-service

# eBPF collector
podman logs rap-ebpf-collector
```

## Cleanup

```bash
# Stop all containers
podman-compose -f infra/podman-compose.yaml down

# Remove volumes (DESTRUCTIVE)
podman-compose -f infra/podman-compose.yaml down -v
```
