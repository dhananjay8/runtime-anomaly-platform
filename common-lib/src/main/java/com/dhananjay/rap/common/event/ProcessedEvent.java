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
public class ProcessedEvent {

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("timestamp")
    private Instant timestamp;

    @JsonProperty("received_at")
    private Instant receivedAt;

    @JsonProperty("container_id")
    private String containerId;

    @JsonProperty("container_name")
    private String containerName;

    @JsonProperty("image_name")
    private String imageName;

    @JsonProperty("namespace")
    private String namespace;

    @JsonProperty("pid")
    private Long pid;

    @JsonProperty("ppid")
    private Long ppid;

    @JsonProperty("syscall")
    private String syscall;

    @JsonProperty("syscall_category")
    private String syscallCategory;

    @JsonProperty("process_name")
    private String processName;

    @JsonProperty("args")
    private String args;

    @JsonProperty("return_value")
    private Long returnValue;

    @JsonProperty("destination_ip")
    private String destinationIp;

    @JsonProperty("source_ip")
    private String sourceIp;

    @JsonProperty("port")
    private Integer port;

    @JsonProperty("protocol")
    private String protocol;

    @JsonProperty("uid")
    private Long uid;

    @JsonProperty("hostname")
    private String hostname;

    @JsonProperty("is_privileged")
    private Boolean isPrivileged;

    @JsonProperty("is_network_event")
    private Boolean isNetworkEvent;
}
