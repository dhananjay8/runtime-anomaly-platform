package com.dhananjay.rap.common.event;

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
public class FeatureVector {

    @JsonProperty("vector_id")
    private String vectorId;

    @JsonProperty("container_id")
    private String containerId;

    @JsonProperty("window_start")
    private Instant windowStart;

    @JsonProperty("window_end")
    private Instant windowEnd;

    @JsonProperty("window_duration_seconds")
    private Integer windowDurationSeconds;

    @JsonProperty("total_events")
    private Long totalEvents;

    // Syscall frequency features
    @JsonProperty("syscall_read_freq")
    private Double syscallReadFreq;

    @JsonProperty("syscall_write_freq")
    private Double syscallWriteFreq;

    @JsonProperty("syscall_open_freq")
    private Double syscallOpenFreq;

    @JsonProperty("syscall_close_freq")
    private Double syscallCloseFreq;

    @JsonProperty("syscall_exec_freq")
    private Double syscallExecFreq;

    @JsonProperty("syscall_connect_freq")
    private Double syscallConnectFreq;

    @JsonProperty("syscall_accept_freq")
    private Double syscallAcceptFreq;

    @JsonProperty("syscall_mmap_freq")
    private Double syscallMmapFreq;

    // Process diversity features
    @JsonProperty("unique_process_count")
    private Integer uniqueProcessCount;

    @JsonProperty("unique_syscall_count")
    private Integer uniqueSyscallCount;

    @JsonProperty("process_creation_rate")
    private Double processCreationRate;

    // Network features
    @JsonProperty("unique_dest_ips")
    private Integer uniqueDestIps;

    @JsonProperty("unique_dest_ports")
    private Integer uniqueDestPorts;

    @JsonProperty("network_event_ratio")
    private Double networkEventRatio;

    // Entropy features
    @JsonProperty("syscall_entropy")
    private Double syscallEntropy;

    @JsonProperty("process_entropy")
    private Double processEntropy;

    // Privilege features
    @JsonProperty("privileged_event_ratio")
    private Double privilegedEventRatio;

    @JsonProperty("uid_zero_ratio")
    private Double uidZeroRatio;

    // All features as a flat array for ML
    @JsonProperty("features")
    private double[] features;

    @JsonProperty("feature_names")
    private String[] featureNames;

    // Additional metadata
    @JsonProperty("syscall_frequencies")
    private Map<String, Long> syscallFrequencies;
}
