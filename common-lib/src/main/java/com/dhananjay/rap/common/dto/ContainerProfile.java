package com.dhananjay.rap.common.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ContainerProfile {

    @JsonProperty("container_id")
    private String containerId;

    @JsonProperty("container_name")
    private String containerName;

    @JsonProperty("image_name")
    private String imageName;

    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("first_seen")
    private Instant firstSeen;

    @JsonProperty("last_seen")
    private Instant lastSeen;

    @JsonProperty("total_events")
    private Long totalEvents;

    @JsonProperty("total_anomalies")
    private Long totalAnomalies;

    @JsonProperty("anomaly_rate")
    private Double anomalyRate;

    @JsonProperty("avg_anomaly_score")
    private Double avgAnomalyScore;

    @JsonProperty("max_anomaly_score")
    private Double maxAnomalyScore;

    @JsonProperty("baseline_syscall_distribution")
    private Map<String, Double> baselineSyscallDistribution;

    @JsonProperty("baseline_process_list")
    private String[] baselineProcessList;

    @JsonProperty("baseline_network_destinations")
    private String[] baselineNetworkDestinations;

    @JsonProperty("risk_level")
    private String riskLevel;
}
