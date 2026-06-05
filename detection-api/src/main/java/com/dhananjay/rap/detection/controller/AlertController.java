package com.dhananjay.rap.detection.controller;

import com.dhananjay.rap.common.dto.AlertRecord;
import com.dhananjay.rap.common.dto.PagedResponse;
import com.dhananjay.rap.detection.service.AlertService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/alerts")
@RequiredArgsConstructor
public class AlertController {

    private final AlertService alertService;

    @GetMapping
    public ResponseEntity<PagedResponse<AlertRecord>> listAlerts(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String severity,
            @RequestParam(required = false) Boolean acknowledged) {
        int cappedSize = Math.min(size, 200);
        return ResponseEntity.ok(alertService.listAlerts(page, cappedSize, severity, acknowledged));
    }

    @GetMapping("/{alertId}")
    public ResponseEntity<AlertRecord> getAlert(@PathVariable String alertId) {
        return alertService.getAlert(alertId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{alertId}/ack")
    public ResponseEntity<Map<String, Object>> acknowledgeAlert(
            @PathVariable String alertId,
            @RequestParam(defaultValue = "system") String acknowledgedBy) {
        boolean updated = alertService.acknowledgeAlert(alertId, acknowledgedBy);
        if (updated) {
            return ResponseEntity.ok(Map.of("alertId", alertId, "acknowledged", true));
        }
        return ResponseEntity.notFound().build();
    }
}
