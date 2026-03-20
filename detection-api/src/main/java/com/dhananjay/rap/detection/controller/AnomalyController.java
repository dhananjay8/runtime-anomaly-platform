package com.dhananjay.rap.detection.controller;

import com.dhananjay.rap.common.dto.AnomalyResponse;
import com.dhananjay.rap.common.dto.PagedResponse;
import com.dhananjay.rap.detection.service.AnomalyService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(name = "Anomaly Detection", description = "APIs for querying runtime anomaly detection results")
public class AnomalyController {

    private final AnomalyService anomalyService;

    @GetMapping("/anomalies")
    @Operation(summary = "List anomalies", description = "Retrieve paginated list of detected anomalies with optional filters")
    public ResponseEntity<PagedResponse<AnomalyResponse>> getAnomalies(
            @Parameter(description = "Page number (0-indexed)") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "20") int size,
            @Parameter(description = "Minimum severity: LOW, MEDIUM, HIGH, CRITICAL") @RequestParam(required = false) String severity,
            @Parameter(description = "Filter by container ID") @RequestParam(required = false) String containerId,
            @Parameter(description = "Start time filter (ISO-8601)") @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @Parameter(description = "End time filter (ISO-8601)") @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {

        log.info("Fetching anomalies: page={}, size={}, severity={}, containerId={}", page, size, severity, containerId);
        PagedResponse<AnomalyResponse> response = anomalyService.getAnomalies(page, size, severity, containerId, from, to);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/anomalies/{resultId}")
    @Operation(summary = "Get anomaly detail", description = "Retrieve a specific anomaly result by ID")
    public ResponseEntity<AnomalyResponse> getAnomalyById(
            @Parameter(description = "Anomaly result ID") @PathVariable String resultId) {

        log.info("Fetching anomaly detail: resultId={}", resultId);
        return anomalyService.getAnomalyById(resultId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/anomalies/stats")
    @Operation(summary = "Get anomaly statistics", description = "Summary statistics of detected anomalies")
    public ResponseEntity<Map<String, Object>> getAnomalyStats(
            @Parameter(description = "Time window in hours") @RequestParam(defaultValue = "24") int hours) {

        log.info("Fetching anomaly stats for last {} hours", hours);
        Map<String, Object> stats = anomalyService.getAnomalyStats(hours);
        return ResponseEntity.ok(stats);
    }
}
