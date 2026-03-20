-- ============================================================================
-- V4: Anomaly Results Table
-- Stores ML model scoring output and detected anomalies
-- ============================================================================

CREATE TABLE anomaly_results (
    result_id               VARCHAR2(64)    PRIMARY KEY,
    vector_id               VARCHAR2(64),
    container_id            VARCHAR2(128)   NOT NULL,
    anomaly_score           NUMBER(10,6)    NOT NULL,
    is_anomalous            NUMBER(1)       DEFAULT 0 NOT NULL,
    severity                VARCHAR2(16),
    model_version           VARCHAR2(64),
    contributing_features   VARCHAR2(1024),
    description             VARCHAR2(2048),
    detected_at             TIMESTAMP       NOT NULL,
    window_start            TIMESTAMP,
    window_end              TIMESTAMP,
    created_at              TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_ar_vector FOREIGN KEY (vector_id) REFERENCES feature_vectors(vector_id)
);

CREATE INDEX idx_ar_container_id ON anomaly_results (container_id);
CREATE INDEX idx_ar_detected_at ON anomaly_results (detected_at);
CREATE INDEX idx_ar_container_det ON anomaly_results (container_id, detected_at);
CREATE INDEX idx_ar_severity ON anomaly_results (severity);
CREATE INDEX idx_ar_is_anomalous ON anomaly_results (is_anomalous);
CREATE INDEX idx_ar_anomalous_sev ON anomaly_results (is_anomalous, severity, detected_at);
CREATE INDEX idx_ar_score ON anomaly_results (anomaly_score);
