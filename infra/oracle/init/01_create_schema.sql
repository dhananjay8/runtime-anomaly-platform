-- ============================================================================
-- Oracle XE Schema Initialization for RAP
-- Runs automatically on first container startup
-- ============================================================================

-- Runtime Events
CREATE TABLE runtime_events (
    event_id            VARCHAR2(64)    PRIMARY KEY,
    event_timestamp     TIMESTAMP       NOT NULL,
    container_id        VARCHAR2(128)   NOT NULL,
    pid                 NUMBER(10),
    ppid                NUMBER(10),
    syscall             VARCHAR2(64)    NOT NULL,
    syscall_id          NUMBER(5),
    process_name        VARCHAR2(256),
    args                CLOB,
    return_value        NUMBER(20),
    destination_ip      VARCHAR2(45),
    source_ip           VARCHAR2(45),
    port                NUMBER(5),
    protocol            VARCHAR2(16),
    uid                 NUMBER(10),
    hostname            VARCHAR2(256),
    image_name          VARCHAR2(512),
    namespace           VARCHAR2(256),
    created_at          TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX idx_re_container_id ON runtime_events (container_id);
CREATE INDEX idx_re_timestamp ON runtime_events (event_timestamp);
CREATE INDEX idx_re_container_ts ON runtime_events (container_id, event_timestamp);
CREATE INDEX idx_re_syscall ON runtime_events (syscall);

-- Processed Events
CREATE TABLE processed_events (
    event_id            VARCHAR2(64)    PRIMARY KEY,
    event_timestamp     TIMESTAMP       NOT NULL,
    received_at         TIMESTAMP       NOT NULL,
    container_id        VARCHAR2(128)   NOT NULL,
    container_name      VARCHAR2(256),
    image_name          VARCHAR2(512),
    namespace           VARCHAR2(256),
    pid                 NUMBER(10),
    ppid                NUMBER(10),
    syscall             VARCHAR2(64)    NOT NULL,
    syscall_category    VARCHAR2(32),
    process_name        VARCHAR2(256),
    args                CLOB,
    return_value        NUMBER(20),
    destination_ip      VARCHAR2(45),
    source_ip           VARCHAR2(45),
    port                NUMBER(5),
    protocol            VARCHAR2(16),
    uid                 NUMBER(10),
    hostname            VARCHAR2(256),
    is_privileged       NUMBER(1)       DEFAULT 0,
    is_network_event    NUMBER(1)       DEFAULT 0,
    created_at          TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX idx_pe_container_id ON processed_events (container_id);
CREATE INDEX idx_pe_timestamp ON processed_events (event_timestamp);
CREATE INDEX idx_pe_container_ts ON processed_events (container_id, event_timestamp);
CREATE INDEX idx_pe_syscall_cat ON processed_events (syscall_category);

-- Feature Vectors
CREATE TABLE feature_vectors (
    vector_id               VARCHAR2(64)    PRIMARY KEY,
    container_id            VARCHAR2(128)   NOT NULL,
    window_start            TIMESTAMP       NOT NULL,
    window_end              TIMESTAMP       NOT NULL,
    window_duration_seconds NUMBER(10),
    total_events            NUMBER(20),
    syscall_read_freq       NUMBER(10,6),
    syscall_write_freq      NUMBER(10,6),
    syscall_open_freq       NUMBER(10,6),
    syscall_close_freq      NUMBER(10,6),
    syscall_exec_freq       NUMBER(10,6),
    syscall_connect_freq    NUMBER(10,6),
    syscall_accept_freq     NUMBER(10,6),
    syscall_mmap_freq       NUMBER(10,6),
    unique_process_count    NUMBER(10),
    unique_syscall_count    NUMBER(10),
    process_creation_rate   NUMBER(10,6),
    unique_dest_ips         NUMBER(10),
    unique_dest_ports       NUMBER(10),
    network_event_ratio     NUMBER(10,6),
    syscall_entropy         NUMBER(10,6),
    process_entropy         NUMBER(10,6),
    privileged_event_ratio  NUMBER(10,6),
    uid_zero_ratio          NUMBER(10,6),
    features_json           CLOB,
    syscall_frequencies_json CLOB,
    created_at              TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX idx_fv_container_id ON feature_vectors (container_id);
CREATE INDEX idx_fv_window_start ON feature_vectors (window_start);
CREATE INDEX idx_fv_container_window ON feature_vectors (container_id, window_start);

-- Anomaly Results
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

-- Container Profiles
CREATE TABLE container_profiles (
    container_id                VARCHAR2(128)   PRIMARY KEY,
    container_name              VARCHAR2(256),
    image_name                  VARCHAR2(512),
    namespace                   VARCHAR2(256),
    first_seen                  TIMESTAMP       NOT NULL,
    last_seen                   TIMESTAMP       NOT NULL,
    total_events                NUMBER(20)      DEFAULT 0,
    total_anomalies             NUMBER(20)      DEFAULT 0,
    anomaly_rate                NUMBER(10,6)    DEFAULT 0,
    avg_anomaly_score           NUMBER(10,6)    DEFAULT 0,
    max_anomaly_score           NUMBER(10,6)    DEFAULT 0,
    baseline_syscall_dist_json  CLOB,
    baseline_process_list       VARCHAR2(4000),
    baseline_network_destinations VARCHAR2(4000),
    risk_level                  VARCHAR2(16)    DEFAULT 'LOW',
    updated_at                  TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX idx_cp_risk_level ON container_profiles (risk_level);
CREATE INDEX idx_cp_last_seen ON container_profiles (last_seen);

-- Alert History
CREATE TABLE alert_history (
    alert_id            VARCHAR2(64)    PRIMARY KEY,
    result_id           VARCHAR2(64),
    container_id        VARCHAR2(128)   NOT NULL,
    severity            VARCHAR2(16)    NOT NULL,
    title               VARCHAR2(512)   NOT NULL,
    description         VARCHAR2(4000),
    anomaly_score       NUMBER(10,6),
    acknowledged        NUMBER(1)       DEFAULT 0,
    acknowledged_by     VARCHAR2(128),
    acknowledged_at     TIMESTAMP,
    created_at          TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT fk_ah_result FOREIGN KEY (result_id) REFERENCES anomaly_results(result_id)
);

CREATE INDEX idx_ah_container_id ON alert_history (container_id);
CREATE INDEX idx_ah_severity ON alert_history (severity);
CREATE INDEX idx_ah_created_at ON alert_history (created_at);
