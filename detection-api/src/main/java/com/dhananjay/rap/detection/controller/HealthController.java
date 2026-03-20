package com.dhananjay.rap.detection.controller;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
@RequiredArgsConstructor
@Tag(name = "Health", description = "Platform health and readiness endpoints")
public class HealthController {

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;

    @GetMapping("/health")
    @Operation(summary = "Health check", description = "Returns platform health status including DB and Kafka connectivity")
    public ResponseEntity<Map<String, Object>> health() {
        Map<String, Object> health = new LinkedHashMap<>();
        health.put("status", "UP");
        health.put("timestamp", Instant.now().toString());

        // Check Oracle DB
        try {
            jdbcTemplate.queryForObject("SELECT 1 FROM DUAL", Integer.class);
            health.put("database", Map.of("status", "UP", "type", "Oracle"));
        } catch (Exception e) {
            health.put("database", Map.of("status", "DOWN", "error", e.getMessage()));
            health.put("status", "DEGRADED");
        }

        // Check Kafka
        try {
            kafkaTemplate.getProducerFactory().createProducer().metrics();
            health.put("kafka", Map.of("status", "UP"));
        } catch (Exception e) {
            health.put("kafka", Map.of("status", "DOWN", "error", e.getMessage()));
            health.put("status", "DEGRADED");
        }

        return ResponseEntity.ok(health);
    }
}
