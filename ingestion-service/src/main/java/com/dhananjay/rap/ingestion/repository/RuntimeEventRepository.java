package com.dhananjay.rap.ingestion.repository;

import com.dhananjay.rap.common.event.RuntimeEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Slf4j
@Repository
@RequiredArgsConstructor
public class RuntimeEventRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_RUNTIME_EVENT = """
            MERGE INTO runtime_events re
            USING (SELECT ? AS event_id FROM DUAL) src
            ON (re.event_id = src.event_id)
            WHEN NOT MATCHED THEN INSERT (
                event_id, event_timestamp, container_id, pid, ppid,
                syscall, syscall_id, process_name, args, return_value,
                destination_ip, source_ip, port, protocol, uid,
                hostname, image_name, namespace
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    public void insertRuntimeEvent(RuntimeEvent event) {
        jdbcTemplate.update(UPSERT_RUNTIME_EVENT,
                event.getEventId(),
                // INSERT values
                event.getEventId(),
                Timestamp.from(event.getTimestamp()),
                event.getContainerId(),
                event.getPid(),
                event.getPpid(),
                event.getSyscall(),
                event.getSyscallId(),
                event.getProcessName(),
                event.getArgs(),
                event.getReturnValue(),
                event.getDestinationIp(),
                event.getSourceIp(),
                event.getPort(),
                event.getProtocol(),
                event.getUid(),
                event.getHostname(),
                event.getImageName(),
                event.getNamespace()
        );
    }

    public long countByContainerId(String containerId) {
        Long count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM runtime_events WHERE container_id = ?",
                Long.class, containerId);
        return count != null ? count : 0;
    }

    public long countAll() {
        Long count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM runtime_events", Long.class);
        return count != null ? count : 0;
    }
}
