package com.dhananjay.rap.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AlertRecord {

    @JsonProperty("alert_id")
    private String alertId;

    @JsonProperty("result_id")
    private String resultId;

    @JsonProperty("container_id")
    private String containerId;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("anomaly_score")
    private Double anomalyScore;

    @JsonProperty("title")
    private String title;

    @JsonProperty("description")
    private String description;

    @JsonProperty("triggered_at")
    private Instant triggeredAt;

    @JsonProperty("created_at")
    private Instant createdAt;

    @JsonProperty("acknowledged")
    private Boolean acknowledged;

    @JsonProperty("acknowledged_at")
    private Instant acknowledgedAt;

    @JsonProperty("acknowledged_by")
    private String acknowledgedBy;
}
