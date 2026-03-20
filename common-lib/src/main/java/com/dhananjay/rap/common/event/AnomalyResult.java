package com.dhananjay.rap.common.event;

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
public class AnomalyResult {

    @JsonProperty("result_id")
    private String resultId;

    @JsonProperty("vector_id")
    private String vectorId;

    @JsonProperty("container_id")
    private String containerId;

    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("anomaly_score")
    private Double anomalyScore;

    @JsonProperty("is_anomalous")
    private Boolean isAnomalous;

    @JsonProperty("severity")
    private Severity severity;

    @JsonProperty("model_version")
    private String modelVersion;

    @JsonProperty("contributing_features")
    private String[] contributingFeatures;

    @JsonProperty("description")
    private String description;

    @JsonProperty("window_start")
    private Instant windowStart;

    @JsonProperty("window_end")
    private Instant windowEnd;

    public enum Severity {
        LOW, MEDIUM, HIGH, CRITICAL
    }
}
