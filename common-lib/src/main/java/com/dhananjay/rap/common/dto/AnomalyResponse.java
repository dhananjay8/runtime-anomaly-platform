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
public class AnomalyResponse {

    @JsonProperty("result_id")
    private String resultId;

    @JsonProperty("container_id")
    private String containerId;

    @JsonProperty("anomaly_score")
    private Double anomalyScore;

    @JsonProperty("is_anomalous")
    private Boolean isAnomalous;

    @JsonProperty("severity")
    private String severity;

    @JsonProperty("description")
    private String description;

    @JsonProperty("detected_at")
    private Instant detectedAt;

    @JsonProperty("window_start")
    private Instant windowStart;

    @JsonProperty("window_end")
    private Instant windowEnd;

    @JsonProperty("contributing_features")
    private String[] contributingFeatures;

    @JsonProperty("model_version")
    private String modelVersion;
}
