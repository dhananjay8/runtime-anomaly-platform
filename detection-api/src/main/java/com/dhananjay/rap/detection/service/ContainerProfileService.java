package com.dhananjay.rap.detection.service;

import com.dhananjay.rap.common.dto.ContainerProfile;
import com.dhananjay.rap.common.dto.PagedResponse;
import com.dhananjay.rap.detection.repository.ContainerProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ContainerProfileService {

    private final ContainerProfileRepository containerProfileRepository;

    public PagedResponse<ContainerProfile> listContainers(int page, int size, String riskLevel) {
        return containerProfileRepository.findContainers(page, size, riskLevel);
    }

    public Optional<ContainerProfile> getContainerProfile(String containerId) {
        return containerProfileRepository.findByContainerId(containerId);
    }
}
