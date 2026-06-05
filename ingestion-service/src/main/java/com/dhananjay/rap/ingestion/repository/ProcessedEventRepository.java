package com.dhananjay.rap.ingestion.repository;

import com.dhananjay.rap.common.event.ProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;

@Slf4j
@Repository
@RequiredArgsConstructor
public class ProcessedEventRepository {

    private final JdbcTemplate jdbcTemplate;

    private static final String UPSERT_PROCESSED_EVENT = """
            MERGE INTO processed_events pe
            USING (SELECT ? AS event_id FROM DUAL) src
            ON (pe.event_id = src.event_id)
            WHEN NOT MATCHED THEN INSERT (
                event_id, container_id, container_name, image_name, namespace,
                event_timestamp, received_at, pid, ppid, syscall, syscall_category,
                process_name, args, return_value, destination_ip, source_ip,
                port, protocol, uid, hostname, is_privileged, is_network_event
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    public void upsertProcessedEvent(ProcessedEvent event) {
        jdbcTemplate.update(UPSERT_PROCESSED_EVENT,
                event.getEventId(),
                // INSERT values
                event.getEventId(),
                event.getContainerId(),
                event.getContainerName(),
                event.getImageName(),
                event.getNamespace(),
                event.getTimestamp() != null ? Timestamp.from(event.getTimestamp()) : null,
                event.getReceivedAt() != null ? Timestamp.from(event.getReceivedAt()) : null,
                event.getPid(),
                event.getPpid(),
                event.getSyscall(),
                event.getSyscallCategory(),
                event.getProcessName(),
                event.getArgs(),
                event.getReturnValue(),
                event.getDestinationIp(),
                event.getSourceIp(),
                event.getPort(),
                event.getProtocol(),
                event.getUid(),
                event.getHostname(),
                Boolean.TRUE.equals(event.getIsPrivileged()) ? 1 : 0,
                Boolean.TRUE.equals(event.getIsNetworkEvent()) ? 1 : 0
        );
    }
}
