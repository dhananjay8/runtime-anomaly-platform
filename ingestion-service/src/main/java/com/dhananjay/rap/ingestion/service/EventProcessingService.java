package com.dhananjay.rap.ingestion.service;

import com.dhananjay.rap.common.constants.KafkaTopics;
import com.dhananjay.rap.common.constants.SyscallCategory;
import com.dhananjay.rap.common.event.ProcessedEvent;
import com.dhananjay.rap.common.event.RuntimeEvent;
import com.dhananjay.rap.common.util.JsonUtil;
import com.dhananjay.rap.ingestion.repository.RuntimeEventRepository;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class EventProcessingService {

    private final RuntimeEventRepository runtimeEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    private static final Set<String> NETWORK_SYSCALLS = Set.of(
            "connect", "accept", "accept4", "bind", "listen",
            "sendto", "recvfrom", "sendmsg", "recvmsg", "socket"
    );

    private static final Set<String> PRIVILEGED_SYSCALLS = Set.of(
            "ptrace", "mount", "umount2", "chroot", "bpf",
            "perf_event_open", "seccomp", "capset"
    );

    @Transactional
    @CircuitBreaker(name = "eventProcessing", fallbackMethod = "processEventFallback")
    public void processEvent(RuntimeEvent rawEvent) {
        if (rawEvent.getEventId() == null) {
            rawEvent.setEventId(UUID.randomUUID().toString());
        }

        validateEvent(rawEvent);

        runtimeEventRepository.insertRuntimeEvent(rawEvent);

        ProcessedEvent processedEvent = enrichEvent(rawEvent);

        String json = JsonUtil.toJson(processedEvent);
        kafkaTemplate.send(KafkaTopics.PROCESSED_EVENTS, processedEvent.getContainerId(), json)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish processed event {}: {}",
                                processedEvent.getEventId(), ex.getMessage());
                    } else {
                        log.debug("Published processed event {} to partition {} offset {}",
                                processedEvent.getEventId(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }

    private void validateEvent(RuntimeEvent event) {
        if (event.getContainerId() == null || event.getContainerId().isBlank()) {
            throw new IllegalArgumentException("container_id is required");
        }
        if (event.getTimestamp() == null) {
            event.setTimestamp(Instant.now());
        }
        if (event.getSyscall() == null || event.getSyscall().isBlank()) {
            throw new IllegalArgumentException("syscall is required");
        }
    }

    private ProcessedEvent enrichEvent(RuntimeEvent raw) {
        String syscall = raw.getSyscall().toLowerCase();

        return ProcessedEvent.builder()
                .eventId(raw.getEventId())
                .timestamp(raw.getTimestamp())
                .receivedAt(Instant.now())
                .containerId(raw.getContainerId())
                .imageName(raw.getImageName())
                .namespace(raw.getNamespace())
                .pid(raw.getPid())
                .ppid(raw.getPpid())
                .syscall(syscall)
                .syscallCategory(SyscallCategory.categorize(syscall))
                .processName(raw.getProcessName())
                .args(raw.getArgs())
                .returnValue(raw.getReturnValue())
                .destinationIp(raw.getDestinationIp())
                .sourceIp(raw.getSourceIp())
                .port(raw.getPort())
                .protocol(raw.getProtocol())
                .uid(raw.getUid())
                .hostname(raw.getHostname())
                .isPrivileged(PRIVILEGED_SYSCALLS.contains(syscall))
                .isNetworkEvent(NETWORK_SYSCALLS.contains(syscall))
                .build();
    }

    @SuppressWarnings("unused")
    private void processEventFallback(RuntimeEvent rawEvent, Throwable throwable) {
        log.error("Circuit breaker open for event processing. Event {} dropped: {}",
                rawEvent.getEventId(), throwable.getMessage());
    }
}
