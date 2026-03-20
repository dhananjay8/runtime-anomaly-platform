-- ============================================================================
-- V2: Processed Events Table
-- Stores normalized and enriched events after ingestion pipeline
-- ============================================================================

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
CREATE INDEX idx_pe_privileged ON processed_events (is_privileged) WHERE is_privileged = 1;
CREATE INDEX idx_pe_network ON processed_events (is_network_event) WHERE is_network_event = 1;
