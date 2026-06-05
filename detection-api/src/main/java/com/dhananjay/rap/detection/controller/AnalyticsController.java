package com.dhananjay.rap.detection.controller;

import com.dhananjay.rap.detection.service.AnalyticsService;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/analytics")
@RequiredArgsConstructor
public class AnalyticsController {

    private final AnalyticsService analyticsService;

    @GetMapping("/trend")
    public ResponseEntity<List<Map<String, Object>>> getAnomalyTrend(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "15") int bucketMinutes) {
        return ResponseEntity.ok(analyticsService.getAnomalyTrend(from, to, bucketMinutes));
    }

    @GetMapping("/severity-over-time")
    public ResponseEntity<List<Map<String, Object>>> getSeverityOverTime(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(defaultValue = "15") int bucketMinutes) {
        return ResponseEntity.ok(analyticsService.getSeverityOverTime(from, to, bucketMinutes));
    }

    @GetMapping("/top-risky-containers")
    public ResponseEntity<List<Map<String, Object>>> getTopRiskyContainers(
            @RequestParam(defaultValue = "24") int hours,
            @RequestParam(defaultValue = "10") int limit) {
        return ResponseEntity.ok(analyticsService.getTopRiskyContainers(hours, limit));
    }
}
