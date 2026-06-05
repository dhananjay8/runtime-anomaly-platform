package com.dhananjay.rap.detection.service;

import com.dhananjay.rap.detection.repository.AnomalyRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnalyticsService {

    private final AnomalyRepository anomalyRepository;

    public List<Map<String, Object>> getAnomalyTrend(Instant from, Instant to, int bucketMinutes) {
        if (from == null) from = Instant.now().minus(24, ChronoUnit.HOURS);
        if (to == null) to = Instant.now();
        int capped = Math.max(1, Math.min(bucketMinutes, 1440));
        return anomalyRepository.getAnomalyTrend(from, to, capped);
    }

    public List<Map<String, Object>> getSeverityOverTime(Instant from, Instant to, int bucketMinutes) {
        if (from == null) from = Instant.now().minus(24, ChronoUnit.HOURS);
        if (to == null) to = Instant.now();
        int capped = Math.max(1, Math.min(bucketMinutes, 1440));
        return anomalyRepository.getSeverityOverTime(from, to, capped);
    }

    public List<Map<String, Object>> getTopRiskyContainers(int hours, int limit) {
        Instant since = Instant.now().minus(hours, ChronoUnit.HOURS);
        return anomalyRepository.getTopRiskyContainers(since, limit);
    }
}
