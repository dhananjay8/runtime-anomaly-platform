-- ============================================================================
-- V5: Container Profiles + Alert History Tables
-- Stores learned behavioral baselines and triggered alerts
-- ============================================================================

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
CREATE INDEX idx_cp_image ON container_profiles (image_name);

-- ============================================================================
-- Alert History Table
-- ============================================================================

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
CREATE INDEX idx_ah_acknowledged ON alert_history (acknowledged);

-- ============================================================================
-- Init Script: Create application user
-- ============================================================================
-- Run as SYSDBA:
-- CREATE USER rap_user IDENTIFIED BY rap_password;
-- GRANT CONNECT, RESOURCE, CREATE TABLE, CREATE SEQUENCE TO rap_user;
-- ALTER USER rap_user QUOTA UNLIMITED ON USERS;
