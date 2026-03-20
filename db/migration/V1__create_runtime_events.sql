-- ============================================================================
-- V1: Runtime Events Table
-- Stores raw eBPF telemetry events from the collector agent
-- ============================================================================

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

-- Partition by range on event_timestamp for high-volume ingestion
-- (Oracle XE does not support partitioning; use indexes instead)

CREATE INDEX idx_re_container_id ON runtime_events (container_id);
CREATE INDEX idx_re_timestamp ON runtime_events (event_timestamp);
CREATE INDEX idx_re_container_ts ON runtime_events (container_id, event_timestamp);
CREATE INDEX idx_re_syscall ON runtime_events (syscall);
CREATE INDEX idx_re_process_name ON runtime_events (process_name);
CREATE INDEX idx_re_created_at ON runtime_events (created_at);
