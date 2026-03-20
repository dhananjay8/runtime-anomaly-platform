-- ============================================================================
-- V3: Feature Vectors Table
-- Stores time-windowed behavioral feature vectors for ML scoring
-- ============================================================================

CREATE TABLE feature_vectors (
    vector_id               VARCHAR2(64)    PRIMARY KEY,
    container_id            VARCHAR2(128)   NOT NULL,
    window_start            TIMESTAMP       NOT NULL,
    window_end              TIMESTAMP       NOT NULL,
    window_duration_seconds NUMBER(10),
    total_events            NUMBER(20),
    -- Syscall frequency features
    syscall_read_freq       NUMBER(10,6),
    syscall_write_freq      NUMBER(10,6),
    syscall_open_freq       NUMBER(10,6),
    syscall_close_freq      NUMBER(10,6),
    syscall_exec_freq       NUMBER(10,6),
    syscall_connect_freq    NUMBER(10,6),
    syscall_accept_freq     NUMBER(10,6),
    syscall_mmap_freq       NUMBER(10,6),
    -- Process diversity features
    unique_process_count    NUMBER(10),
    unique_syscall_count    NUMBER(10),
    process_creation_rate   NUMBER(10,6),
    -- Network features
    unique_dest_ips         NUMBER(10),
    unique_dest_ports       NUMBER(10),
    network_event_ratio     NUMBER(10,6),
    -- Entropy features
    syscall_entropy         NUMBER(10,6),
    process_entropy         NUMBER(10,6),
    -- Privilege features
    privileged_event_ratio  NUMBER(10,6),
    uid_zero_ratio          NUMBER(10,6),
    -- Raw data
    features_json           CLOB,
    syscall_frequencies_json CLOB,
    created_at              TIMESTAMP       DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE INDEX idx_fv_container_id ON feature_vectors (container_id);
CREATE INDEX idx_fv_window_start ON feature_vectors (window_start);
CREATE INDEX idx_fv_container_window ON feature_vectors (container_id, window_start);
CREATE INDEX idx_fv_created_at ON feature_vectors (created_at);
