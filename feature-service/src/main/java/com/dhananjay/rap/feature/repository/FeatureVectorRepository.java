package com.dhananjay.rap.feature.repository;

import com.dhananjay.rap.common.event.FeatureVector;
import com.dhananjay.rap.common.util.JsonUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Slf4j
@Repository
@RequiredArgsConstructor
public class FeatureVectorRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String INSERT_FEATURE_VECTOR = """
            INSERT INTO feature_vectors (
                vector_id, container_id, window_start, window_end,
                window_duration_seconds, total_events,
                syscall_read_freq, syscall_write_freq, syscall_open_freq,
                syscall_close_freq, syscall_exec_freq, syscall_connect_freq,
                syscall_accept_freq, syscall_mmap_freq,
                unique_process_count, unique_syscall_count, process_creation_rate,
                unique_dest_ips, unique_dest_ports, network_event_ratio,
                syscall_entropy, process_entropy,
                privileged_event_ratio, uid_zero_ratio,
                features_json, syscall_frequencies_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    public void insertFeatureVector(FeatureVector vector) {
        jdbcTemplate.update(INSERT_FEATURE_VECTOR,
                vector.getVectorId(),
                vector.getContainerId(),
                Timestamp.from(vector.getWindowStart()),
                Timestamp.from(vector.getWindowEnd()),
                vector.getWindowDurationSeconds(),
                vector.getTotalEvents(),
                vector.getSyscallReadFreq(),
                vector.getSyscallWriteFreq(),
                vector.getSyscallOpenFreq(),
                vector.getSyscallCloseFreq(),
                vector.getSyscallExecFreq(),
                vector.getSyscallConnectFreq(),
                vector.getSyscallAcceptFreq(),
                vector.getSyscallMmapFreq(),
                vector.getUniqueProcessCount(),
                vector.getUniqueSyscallCount(),
                vector.getProcessCreationRate(),
                vector.getUniqueDestIps(),
                vector.getUniqueDestPorts(),
                vector.getNetworkEventRatio(),
                vector.getSyscallEntropy(),
                vector.getProcessEntropy(),
                vector.getPrivilegedEventRatio(),
                vector.getUidZeroRatio(),
                JsonUtil.toJson(vector.getFeatures()),
                JsonUtil.toJson(vector.getSyscallFrequencies())
        );
    }
}
