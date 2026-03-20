package com.dhananjay.rap.detection.controller;

import com.dhananjay.rap.common.dto.ContainerProfile;
import com.dhananjay.rap.common.dto.PagedResponse;
import com.dhananjay.rap.detection.service.ContainerProfileService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/api/v1/containers")
@RequiredArgsConstructor
@Tag(name = "Container Profiles", description = "APIs for querying container behavioral profiles")
public class ContainerController {

    private final ContainerProfileService containerProfileService;

    @GetMapping
    @Operation(summary = "List containers", description = "Retrieve paginated list of monitored containers")
    public ResponseEntity<PagedResponse<ContainerProfile>> listContainers(
            @Parameter(description = "Page number") @RequestParam(defaultValue = "0") int page,
            @Parameter(description = "Page size") @RequestParam(defaultValue = "20") int size,
            @Parameter(description = "Filter by risk level") @RequestParam(required = false) String riskLevel) {

        log.info("Listing containers: page={}, size={}, riskLevel={}", page, size, riskLevel);
        PagedResponse<ContainerProfile> response = containerProfileService.listContainers(page, size, riskLevel);
        return ResponseEntity.ok(response);
    }

    @GetMapping("/{containerId}/profile")
    @Operation(summary = "Get container profile", description = "Retrieve behavioral fingerprint for a specific container")
    public ResponseEntity<ContainerProfile> getContainerProfile(
            @Parameter(description = "Container ID") @PathVariable String containerId) {

        log.info("Fetching container profile: containerId={}", containerId);
        return containerProfileService.getContainerProfile(containerId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }
}
