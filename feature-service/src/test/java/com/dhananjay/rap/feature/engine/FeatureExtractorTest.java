package com.dhananjay.rap.feature.engine;

import com.dhananjay.rap.common.event.FeatureVector;
import com.dhananjay.rap.common.event.ProcessedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class FeatureExtractorTest {

    private FeatureExtractor extractor;
    private Instant windowStart;
    private Instant windowEnd;

    @BeforeEach
    void setUp() {
        extractor = new FeatureExtractor();
        windowStart = Instant.parse("2026-03-20T12:00:00Z");
        windowEnd = Instant.parse("2026-03-20T12:00:30Z");
    }

    @Test
    void shouldExtractFeaturesFromNormalEvents() {
        List<ProcessedEvent> events = createNormalEvents(100);
        FeatureVector vector = extractor.extract("container-01", events, windowStart, windowEnd);

        assertThat(vector.getVectorId()).isNotNull();
        assertThat(vector.getContainerId()).isEqualTo("container-01");
        assertThat(vector.getTotalEvents()).isEqualTo(100);
        assertThat(vector.getWindowStart()).isEqualTo(windowStart);
        assertThat(vector.getWindowEnd()).isEqualTo(windowEnd);
        assertThat(vector.getFeatures()).hasSize(18);
        assertThat(vector.getFeatureNames()).hasSize(18);
    }

    @Test
    void shouldCalculateSyscallFrequencies() {
        List<ProcessedEvent> events = new ArrayList<>();
        for (int i = 0; i < 50; i++) events.add(createEvent("read", "nginx", false, false));
        for (int i = 0; i < 30; i++) events.add(createEvent("write", "nginx", false, false));
        for (int i = 0; i < 20; i++) events.add(createEvent("open", "nginx", false, false));

        FeatureVector vector = extractor.extract("container-01", events, windowStart, windowEnd);

        assertThat(vector.getSyscallReadFreq()).isCloseTo(0.5, org.assertj.core.data.Offset.offset(0.01));
        assertThat(vector.getSyscallWriteFreq()).isCloseTo(0.3, org.assertj.core.data.Offset.offset(0.01));
        assertThat(vector.getSyscallOpenFreq()).isCloseTo(0.2, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void shouldCalculateProcessDiversity() {
        List<ProcessedEvent> events = new ArrayList<>();
        for (int i = 0; i < 30; i++) events.add(createEvent("read", "nginx", false, false));
        for (int i = 0; i < 30; i++) events.add(createEvent("read", "python3", false, false));
        for (int i = 0; i < 30; i++) events.add(createEvent("read", "redis-server", false, false));

        FeatureVector vector = extractor.extract("container-01", events, windowStart, windowEnd);

        assertThat(vector.getUniqueProcessCount()).isEqualTo(3);
        assertThat(vector.getUniqueSyscallCount()).isEqualTo(1);
    }

    @Test
    void shouldDetectPrivilegedActivity() {
        List<ProcessedEvent> events = new ArrayList<>();
        for (int i = 0; i < 80; i++) events.add(createEvent("read", "nginx", false, false));
        for (int i = 0; i < 20; i++) events.add(createEvent("ptrace", "sh", true, false));

        FeatureVector vector = extractor.extract("container-01", events, windowStart, windowEnd);

        assertThat(vector.getPrivilegedEventRatio()).isCloseTo(0.2, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void shouldCalculateNetworkEventRatio() {
        List<ProcessedEvent> events = new ArrayList<>();
        for (int i = 0; i < 70; i++) events.add(createEvent("read", "nginx", false, false));
        for (int i = 0; i < 30; i++) events.add(createEvent("connect", "nginx", false, true));

        FeatureVector vector = extractor.extract("container-01", events, windowStart, windowEnd);

        assertThat(vector.getNetworkEventRatio()).isCloseTo(0.3, org.assertj.core.data.Offset.offset(0.01));
    }

    @Test
    void shouldCalculateEntropy() {
        // Single syscall = 0 entropy
        List<ProcessedEvent> singleType = new ArrayList<>();
        for (int i = 0; i < 100; i++) singleType.add(createEvent("read", "nginx", false, false));
        FeatureVector singleVector = extractor.extract("c1", singleType, windowStart, windowEnd);
        assertThat(singleVector.getSyscallEntropy()).isCloseTo(0.0, org.assertj.core.data.Offset.offset(0.01));

        // Multiple syscalls = higher entropy
        List<ProcessedEvent> multiType = new ArrayList<>();
        for (int i = 0; i < 25; i++) multiType.add(createEvent("read", "nginx", false, false));
        for (int i = 0; i < 25; i++) multiType.add(createEvent("write", "nginx", false, false));
        for (int i = 0; i < 25; i++) multiType.add(createEvent("open", "nginx", false, false));
        for (int i = 0; i < 25; i++) multiType.add(createEvent("close", "nginx", false, false));
        FeatureVector multiVector = extractor.extract("c2", multiType, windowStart, windowEnd);
        assertThat(multiVector.getSyscallEntropy()).isGreaterThan(1.0);
    }

    @Test
    void shouldHandleEmptyEventList() {
        FeatureVector vector = extractor.extract("container-empty", List.of(), windowStart, windowEnd);

        assertThat(vector.getTotalEvents()).isEqualTo(0);
        assertThat(vector.getFeatures()).hasSize(18);
        for (double f : vector.getFeatures()) {
            assertThat(f).isEqualTo(0.0);
        }
    }

    private List<ProcessedEvent> createNormalEvents(int count) {
        List<ProcessedEvent> events = new ArrayList<>();
        String[] syscalls = {"read", "write", "open", "close", "mmap"};
        for (int i = 0; i < count; i++) {
            events.add(createEvent(syscalls[i % syscalls.length], "nginx", false, false));
        }
        return events;
    }

    private ProcessedEvent createEvent(String syscall, String processName,
                                        boolean privileged, boolean network) {
        return ProcessedEvent.builder()
                .eventId("evt-" + System.nanoTime())
                .timestamp(windowStart)
                .containerId("container-01")
                .pid(1234L)
                .syscall(syscall)
                .processName(processName)
                .isPrivileged(privileged)
                .isNetworkEvent(network)
                .uid(privileged ? 0L : 1000L)
                .build();
    }
}
