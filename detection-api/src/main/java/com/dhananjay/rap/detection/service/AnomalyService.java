package com.dhananjay.rap.detection.service;

import com.dhananjay.rap.common.dto.AnomalyResponse;
import com.dhananjay.rap.common.dto.PagedResponse;
import com.dhananjay.rap.detection.repository.AnomalyRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class AnomalyService {

    private final AnomalyRepository anomalyRepository;

    public PagedResponse<AnomalyResponse> getAnomalies(int page, int size, String severity,
                                                         String containerId, Instant from, Instant to) {
        if (from == null) {
            from = Instant.now().minus(24, ChronoUnit.HOURS);
        }
        if (to == null) {
            to = Instant.now();
        }
        return anomalyRepository.findAnomalies(page, size, severity, containerId, from, to);
    }

    public Optional<AnomalyResponse> getAnomalyById(String resultId) {
        return anomalyRepository.findById(resultId);
    }

    public Map<String, Object> getAnomalyStats(int hours) {
        Instant since = Instant.now().minus(hours, ChronoUnit.HOURS);
        return anomalyRepository.getStats(since);
    }
}
