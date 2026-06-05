# Runtime Anomaly Platform — Improvement Plan

This plan captures a code-grounded analysis of the current `runtime-anomaly-platform`
codebase and a prioritized roadmap to improve it **functionally** and **technically**.

References point to real files/lines in the repo as of this analysis.

---

## 1. Current State Summary

**Pipeline:** `eBPF Collector → runtime-events → Ingestion → processed-events → Feature Service → feature-vectors → ML Service → anomaly-results → Detection API`.

**Modules:**
- `common-lib` — DTOs, events, `KafkaTopics`, `SyscallCategory`, `JsonUtil`.
- `ingestion-service` (8081) — validates/enriches, persists `runtime_events`, republishes.
- `feature-service` (8082) — sliding-window feature extraction, persists `feature_vectors`.
- `detection-api` (8083) — REST query API + consumes `anomaly-results` into `anomaly_results`.
- `ml-service` (8084, Python/Flask) — Isolation Forest scoring + retraining.
- `ebpf-collector` (Python/BCC) — kernel telemetry, simulation mode on non-Linux.
- `infra/` — single-broker Kafka + Zookeeper + Oracle XE via podman-compose.

**What works well:**
- Clean event-driven decomposition with per-stage topics + DLT topics.
- Idempotent producers + manual offset commit + `DefaultErrorHandler`/DLT in Java services.
- Resilience4j circuit breaker around DB/Kafka in `EventProcessingService`.
- Prometheus metrics + structured logging in the ML service.

---

## 2. Key Findings

### 2.1 Functional gaps (features that are advertised but broken/missing)

1. **Container profiles are never populated (dead code).**
   `ContainerProfileRepository.upsertProfile(...)` (`detection-api/.../repository/ContainerProfileRepository.java:89`)
   is never called anywhere. The only consumer in `detection-api` is `AnomalyResultConsumer`,
   which writes `anomaly_results` only. As a result, `GET /api/v1/containers` and
   `GET /api/v1/containers/{id}/profile` always return empty, despite being documented in the README.

2. **Alerting does not exist.**
   The `alert_history` table (`infra/oracle/init/01_create_schema.sql:150`) with
   `acknowledged*` workflow columns is never written to. There is no alert generation,
   no notification channel (email/Slack/webhook/PagerDuty), and no acknowledge endpoint,
   even though the README and severity thresholds imply an alerting capability.

3. **`processed_events` table is unused.**
   `EventProcessingService` persists `runtime_events` and publishes the enriched event to
   Kafka, but nothing writes the `processed_events` table (defined with 4 indexes). The
   table is dead schema or the persistence step is missing.

4. **Detection API is read-only with no history/trend endpoints.**
   No time-series/bucketed trend endpoint, no per-severity breakdown over time, no
   container risk ranking endpoint backed by real profile data, no alert endpoints.

5. **eBPF collector container-id extraction is a stub.**
   `get_container_id` in `ebpf-collector/collector.py` returns an empty buffer; real
   (non-simulated) mode would emit events without a kernel-derived `container_id`.

### 2.2 Technical gaps

1. **No authentication/authorization anywhere.**
   REST APIs (8081/8082/8083), the ML Flask API (8084, including `POST /model/retrain`
   and `POST /score`), Kafka (PLAINTEXT), and Oracle are all unauthenticated. Swagger UI
   is open. `POST /model/retrain` is an unauthenticated, expensive operation.

2. **Hardcoded credentials.**
   `rap_user/rap_password` are committed in every `application.yml`
   (e.g. `detection-api/src/main/resources/application.yml:9`) and in
   `infra/podman-compose.yaml`. No externalized secrets / env-var injection / Vault.

3. **Minimal test coverage.**
   Only 3 test files exist (`common-lib` x2, `feature-service` x1). `ingestion-service`,
   `detection-api`, `ml-service`, and `ebpf-collector` have **zero** tests. No integration
   tests (no Testcontainers for Kafka/Oracle), no contract tests for the cross-language
   event/feature schema.

4. **At-least-once + non-atomic DB/Kafka write can duplicate or diverge.**
   `EventProcessingService.processEvent` is `@Transactional` over a DB insert, but the
   Kafka publish is fire-and-forget in `whenComplete`. On a batch reprocess (any record in
   the batch throwing), already-inserted rows are retried; `runtime_events.event_id` is a
   PK so duplicates throw, which can drive the whole batch to the DLT. There is no
   idempotent upsert / dedup and no transactional outbox.

5. **Cross-language schema is implicit and fragile.**
   `FeatureExtractor.FEATURE_NAMES` (Java) must match `ml-service/app.py` `FEATURE_NAMES`
   by hand. Any drift silently corrupts scoring. No shared schema (Avro/Protobuf/JSON
   Schema) and no schema registry.

6. **ML scoring quality and lifecycle are weak.**
   - Score normalization `max(0, min(1, raw + 0.5))` is uncalibrated and arbitrary.
   - `contributing_features` ranks by raw feature magnitude, not actual model contribution
     (misleading explanations).
   - Single global model for all containers/images — no per-image baseline.
   - Training buffer is in-memory only (lost on restart); cold start scores everything LOW.
   - No model registry, validation gates, drift detection, or A/B of model versions.

7. **ML service runs the Flask dev server in "production."**
   `app.run(...)` (`ml-service/app.py:504`) is single-process/dev-grade; no gunicorn/uvicorn.
   The detector `_lock` is held during `train()`, blocking scoring during retrain.

8. **Processing-time windowing instead of event-time.**
   `FeatureEngineeringService` opens windows at `Instant.now()` rather than event timestamps,
   so out-of-order/replayed events are mis-windowed and results are non-deterministic on replay.

9. **Schema is duplicated and unmanaged.**
   `db/migration/V1..V5` (Flyway-style) exist but no Flyway/Liquibase dependency is wired;
   the runtime schema comes only from `infra/oracle/init/01_create_schema.sql`. Two sources
   of truth → drift risk; no versioned migrations in CI.

10. **Infra is single-node and not production-shaped.**
    Single Kafka broker, `replication-factor=1`, Zookeeper (not KRaft),
    `KAFKA_AUTO_CREATE_TOPICS_ENABLE=true` conflicting with explicit `NewTopic` beans.
    No Dockerfiles for the Spring services; no Kubernetes/Helm manifests.

11. **Observability is partial.**
    OpenTelemetry is on the classpath (`detection-api/build.gradle.kts:26`) but no exporter
    is configured; no distributed tracing/correlation IDs across services; ML Prometheus
    metrics have no scrape config or dashboards.

12. **API robustness.**
    No max `size` cap on paginated endpoints (memory risk), no rate limiting, stats use full
    `COUNT(*)` scans, no global `@RestControllerAdvice` error model, no request validation
    beyond minimal null checks.

13. **No CI/CD.**
    No pipeline for build/test/coverage/lint/SAST/dependency scanning.

---

## 3. Prioritized Roadmap

Priorities: **P0** = correctness/security blockers, **P1** = reliability/quality,
**P2** = ML and product depth, **P3** = scale/ops maturity.

### P0 — Correctness & Security (highest value, low/medium effort)

| # | Item | Type | Files |
|---|------|------|-------|
| P0.1 | **Wire container profile aggregation.** Add a consumer/scheduled job in `detection-api` that calls `upsertProfile`, computing `total_events`, `total_anomalies`, `anomaly_rate`, avg/max score, and `risk_level`. | Functional | `detection-api` (new `ProfileAggregationService`), `ContainerProfileRepository` |
| P0.2 | **Implement alerting.** On `is_anomalous` (>= HIGH), insert `alert_history` and emit a notification (pluggable webhook/Slack). Add `GET /api/v1/alerts` + `POST /api/v1/alerts/{id}/ack`. | Functional | `detection-api` |
| P0.3 | **Persist or remove `processed_events`.** Either write the table in ingestion or drop the dead schema + indexes. | Functional | `ingestion-service`, schema |
| P0.4 | **Add API authentication.** API key or OAuth2/JWT on all REST services; protect ML `POST /model/retrain` and `POST /score`; lock down Swagger in non-dev. | Technical | all services |
| P0.5 | **Externalize secrets.** Move DB/Kafka creds to env vars / Vault; remove hardcoded creds from `application.yml` and compose; provide `.env.example`. | Technical | all `application.yml`, `infra` |
| P0.6 | **Idempotent ingestion.** Use `MERGE`/`INSERT ... ON CONFLICT`-style upsert on `event_id` (or dedup) so DLT redelivery doesn't loop on PK violations. | Technical | `RuntimeEventRepository`, `EventProcessingService` |

### P1 — Reliability & Testing

| # | Item | Type | Files |
|---|------|------|-------|
| P1.1 | **Integration tests with Testcontainers** (Kafka + Oracle) for each Java service; end-to-end pipeline test. | Technical | all Java modules |
| P1.2 | **Unit tests** for `EventProcessingService`, `AnomalyRepository`, `ContainerProfileRepository`, controllers; **pytest** for `ml-service` (`AnomalyDetector`, severity, pipeline) and collector simulator. | Technical | all modules |
| P1.3 | **Flyway/Liquibase** wired as the single schema source of truth; remove the duplicated init script (or generate it from migrations). | Technical | root build, `db/migration` |
| P1.4 | **Global error handling + validation.** `@RestControllerAdvice`, Bean Validation on query params, cap page `size`, standard error envelope. | Technical | `detection-api` |
| P1.5 | **Transactional outbox (or sync send)** to make DB + Kafka publish atomic in ingestion/feature services. | Technical | `ingestion-service`, `feature-service` |
| P1.6 | **CI pipeline**: build + test + JaCoCo coverage gate + Spotless/Checkstyle + `ruff/black` + dependency scan. | Technical | new `.github/workflows` or Jenkins |

### P2 — ML Quality & Product Depth

| # | Item | Type | Files |
|---|------|------|-------|
| P2.1 | **Shared event/feature schema** (Avro/JSON Schema) + schema registry; generate `FEATURE_NAMES` from one source to kill Java/Python drift. | Technical | `common-lib`, `ml-service` |
| P2.2 | **Calibrated scoring + real explanations** (e.g., score calibration, SHAP/feature-deviation-from-baseline instead of raw magnitude). | Functional/ML | `ml-service` |
| P2.3 | **Per-image/namespace baselining** and persistent training buffer (replay from `feature_vectors` on startup). | Functional/ML | `ml-service`, DB |
| P2.4 | **Model registry + validation gates + drift detection**; expose model lineage in `/model/info`. | Technical/ML | `ml-service` |
| P2.5 | **Event-time windowing** keyed on event timestamps with allowed lateness; deterministic on replay. | Technical | `feature-service` |
| P2.6 | **Trend/analytics endpoints** (time-bucketed anomaly counts, severity over time, top risky containers). | Functional | `detection-api` |
| P2.7 | **Production WSGI** for ML (gunicorn), separate scoring threads from training; non-blocking retrain. | Technical | `ml-service` |

### P3 — Scale & Operational Maturity

| # | Item | Type | Files |
|---|------|------|-------|
| P3.1 | **Dockerfiles for Spring services** + full one-command compose; **Helm/K8s** manifests. | Technical | new |
| P3.2 | **Kafka hardening**: KRaft or multi-broker, RF>=3 in prod profile, disable auto-create, SASL/TLS. | Technical | `infra`, Kafka config |
| P3.3 | **Distributed tracing** (OTel exporter + collector), correlation IDs propagated via Kafka headers; Grafana dashboards + Prometheus scrape config. | Technical | all services, `infra` |
| P3.4 | **Data lifecycle**: partitioning/retention for `runtime_events`/`processed_events`, archival, PII review of `args`. | Technical | DB |
| P3.5 | **Real eBPF path**: implement cgroup→container-id resolution; CO-RE/libbpf option; document kernel reqs. | Functional | `ebpf-collector` |
| P3.6 | **Rate limiting / API gateway**, multi-tenant isolation if applicable. | Technical | `detection-api` |

---

## 4. Suggested Sequencing

1. **Sprint 1 (P0):** profiles aggregation, alerting MVP, auth + secrets, idempotent ingestion, decide `processed_events`. Delivers working advertised features + closes security holes.
2. **Sprint 2 (P1):** Testcontainers + unit tests, Flyway, error handling, outbox, CI.
3. **Sprint 3 (P2):** shared schema/registry, ML scoring/explanations/baselining, event-time windows, trends API, gunicorn.
4. **Sprint 4 (P3):** containerize all services, Kafka/infra hardening, tracing/dashboards, data lifecycle, real eBPF.

---

## 5. Success Criteria

- `GET /api/v1/containers` and `/containers/{id}/profile` return populated, accurate data.
- Anomalies at/above HIGH create `alert_history` rows and fire a notification; alerts can be acknowledged.
- No credentials in source; all services require auth; `POST /model/retrain` is protected.
- Ingestion is idempotent: replaying a batch produces no duplicate rows and no DLT loop.
- Coverage gate (e.g., >=70%) enforced in CI; Testcontainers e2e pipeline test passes.
- Java and Python feature definitions are generated from one schema (no manual drift).
- ML produces calibrated scores with model-grounded contributing-feature explanations.
- Each service has a Dockerfile and a single command brings up the full stack.

---

## 6. Quick Wins (can land immediately)

- Cap `size` on paginated endpoints and add `@RestControllerAdvice`.
- Set `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` (topics are created by `NewTopic` beans / `kafka-init`).
- Replace Flask dev server with gunicorn in the ML Dockerfile.
- Move `rap_password`/datasource creds to env vars with sane defaults for local only.
- Delete or implement `processed_events` to remove dead schema ambiguity.
- Add `@RestControllerAdvice` + `Optional` 404 already used — extend consistently.
