package com.dhananjay.rap.detection.service;

import com.dhananjay.rap.common.dto.AlertRecord;
import com.dhananjay.rap.common.dto.PagedResponse;
import com.dhananjay.rap.common.event.AnomalyResult;
import com.dhananjay.rap.detection.repository.AlertRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class AlertService {

    private final AlertRepository alertRepository;

    @Value("${rap.alerting.webhook-url:}")
    private String webhookUrl;

    private final RestTemplate restTemplate = new RestTemplate();

    public void evaluateAndAlert(AnomalyResult result) {
        AnomalyResult.Severity severity = result.getSeverity();
        if (severity == null) return;
        if (severity == AnomalyResult.Severity.LOW || severity == AnomalyResult.Severity.MEDIUM) return;

        AlertRecord alert = AlertRecord.builder()
                .alertId(UUID.randomUUID().toString())
                .resultId(result.getResultId())
                .containerId(result.getContainerId())
                .severity(severity.name())
                .anomalyScore(result.getAnomalyScore())
                .title(buildTitle(result))
                .description(buildDescription(result))
                .triggeredAt(Instant.now())
                .acknowledged(false)
                .build();

        alertRepository.insertAlert(alert);
        log.warn("Alert created: alertId={} containerId={} severity={} score={}",
                alert.getAlertId(), alert.getContainerId(), alert.getSeverity(), alert.getAnomalyScore());

        if (webhookUrl != null && !webhookUrl.isBlank()) {
            dispatchWebhook(alert);
        }
    }

    public PagedResponse<AlertRecord> listAlerts(int page, int size, String severity, Boolean acknowledged) {
        return alertRepository.findAlerts(page, size, severity, acknowledged);
    }

    public Optional<AlertRecord> getAlert(String alertId) {
        return alertRepository.findByAlertId(alertId);
    }

    public boolean acknowledgeAlert(String alertId, String acknowledgedBy) {
        boolean updated = alertRepository.acknowledge(alertId, acknowledgedBy);
        if (updated) {
            log.info("Alert acknowledged: alertId={} by={}", alertId, acknowledgedBy);
        }
        return updated;
    }

    private String buildTitle(AnomalyResult result) {
        return String.format("[%s] Anomaly detected on container %s",
                result.getSeverity() != null ? result.getSeverity().name() : "UNKNOWN",
                result.getContainerId());
    }

    private String buildDescription(AnomalyResult result) {
        return String.format("score=%.4f severity=%s resultId=%s",
                result.getAnomalyScore() != null ? result.getAnomalyScore() : 0.0,
                result.getSeverity() != null ? result.getSeverity().name() : "UNKNOWN",
                result.getResultId());
    }

    private void dispatchWebhook(AlertRecord alert) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            Map<String, Object> payload = Map.of(
                    "alert_id", alert.getAlertId(),
                    "container_id", alert.getContainerId(),
                    "severity", alert.getSeverity(),
                    "anomaly_score", alert.getAnomalyScore(),
                    "title", alert.getTitle(),
                    "triggered_at", alert.getTriggeredAt().toString()
            );
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, headers);
            restTemplate.postForEntity(webhookUrl, request, String.class);
            log.info("Webhook dispatched for alertId={}", alert.getAlertId());
        } catch (Exception e) {
            log.warn("Failed to dispatch webhook for alertId={}: {}", alert.getAlertId(), e.getMessage());
        }
    }
}
