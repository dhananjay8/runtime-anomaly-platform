package com.dhananjay.rap.feature.service;

import com.dhananjay.rap.common.constants.KafkaTopics;
import com.dhananjay.rap.common.event.FeatureVector;
import com.dhananjay.rap.common.event.ProcessedEvent;
import com.dhananjay.rap.common.util.JsonUtil;
import com.dhananjay.rap.feature.engine.FeatureExtractor;
import com.dhananjay.rap.feature.repository.FeatureVectorRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class FeatureEngineeringService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final FeatureVectorRepository featureVectorRepository;
    private final FeatureExtractor featureExtractor;

    @Value("${feature.window.duration-seconds:30}")
    private int windowDurationSeconds;

    @Value("${feature.window.max-events:10000}")
    private int maxEventsPerWindow;

    @Value("${feature.window.allowed-lateness-seconds:10}")
    private int allowedLatenessSeconds;

    // Key: "containerId|windowStartEpochSeconds"
    private final ConcurrentHashMap<String, WindowAccumulator> activeWindows = new ConcurrentHashMap<>();

    private Instant computeEventTimeBucket(Instant eventTime) {
        long epochSeconds = eventTime.getEpochSecond();
        long bucketStart = (epochSeconds / windowDurationSeconds) * windowDurationSeconds;
        return Instant.ofEpochSecond(bucketStart);
    }

    private String windowKey(String containerId, Instant bucketStart) {
        return containerId + "|" + bucketStart.getEpochSecond();
    }

    public void accumulateEvent(ProcessedEvent event) {
        Instant eventTime = event.getTimestamp() != null ? event.getTimestamp() : Instant.now();
        Instant bucketStart = computeEventTimeBucket(eventTime);
        String key = windowKey(event.getContainerId(), bucketStart);

        WindowAccumulator window = activeWindows.computeIfAbsent(key,
                k -> new WindowAccumulator(event.getContainerId(), bucketStart, windowDurationSeconds));

        window.addEvent(event);

        if (window.getEventCount() >= maxEventsPerWindow) {
            WindowAccumulator flushed = activeWindows.remove(key);
            if (flushed != null) {
                flushWindow(event.getContainerId(), flushed);
            }
        }
    }

    public void flushExpiredWindows() {
        Instant now = Instant.now();
        Instant watermark = now.minusSeconds(allowedLatenessSeconds);

        List<String> expiredKeys = activeWindows.entrySet().stream()
                .filter(e -> e.getValue().isPastAllowedLateness(watermark))
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        for (String key : expiredKeys) {
            WindowAccumulator window = activeWindows.remove(key);
            if (window != null && window.getEventCount() > 0) {
                flushWindow(window.getContainerId(), window);
            }
        }

        if (!expiredKeys.isEmpty()) {
            log.info("Flushed {} expired event-time windows", expiredKeys.size());
        }
    }

    @Scheduled(fixedDelayString = "${feature.window.flush-interval-ms:5000}")
    public void scheduledFlush() {
        flushExpiredWindows();
    }

    private void flushWindow(String containerId, WindowAccumulator window) {
        try {
            FeatureVector vector = featureExtractor.extract(
                    containerId,
                    window.getEvents(),
                    window.getWindowStart(),
                    window.getWindowEnd()
            );

            featureVectorRepository.insertFeatureVector(vector);

            String json = JsonUtil.toJson(vector);
            kafkaTemplate.send(KafkaTopics.FEATURE_VECTORS, containerId, json)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Failed to publish feature vector for container {}: {}",
                                    containerId, ex.getMessage());
                        } else {
                            log.debug("Published feature vector {} for container {}",
                                    vector.getVectorId(), containerId);
                        }
                    });
        } catch (Exception e) {
            log.error("Failed to extract features for container {}: {}", containerId, e.getMessage(), e);
        }
    }

    public static class WindowAccumulator {
        private final String containerId;
        private final Instant windowStart;
        private final int durationSeconds;
        private final List<ProcessedEvent> events = Collections.synchronizedList(new ArrayList<>());

        public WindowAccumulator(String containerId, Instant windowStart, int durationSeconds) {
            this.containerId = containerId;
            this.windowStart = windowStart;
            this.durationSeconds = durationSeconds;
        }

        public void addEvent(ProcessedEvent event) {
            events.add(event);
        }

        public int getEventCount() {
            return events.size();
        }

        public List<ProcessedEvent> getEvents() {
            return Collections.unmodifiableList(events);
        }

        public String getContainerId() {
            return containerId;
        }

        public Instant getWindowStart() {
            return windowStart;
        }

        public Instant getWindowEnd() {
            return windowStart.plus(Duration.ofSeconds(durationSeconds));
        }

        public boolean isPastAllowedLateness(Instant watermark) {
            return watermark.isAfter(getWindowEnd());
        }
    }
}
