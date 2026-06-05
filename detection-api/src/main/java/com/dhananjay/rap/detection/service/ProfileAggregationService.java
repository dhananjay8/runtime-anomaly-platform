package com.dhananjay.rap.detection.service;

import com.dhananjay.rap.common.dto.ContainerProfile;
import com.dhananjay.rap.common.event.AnomalyResult;
import com.dhananjay.rap.detection.repository.ContainerProfileRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProfileAggregationService {

    private final ContainerProfileRepository containerProfileRepository;

    public void updateProfile(AnomalyResult result) {
        if (result.getContainerId() == null) return;

        Optional<ContainerProfile> existing = containerProfileRepository.findByContainerId(result.getContainerId());

        ContainerProfile profile;
        if (existing.isPresent()) {
            profile = existing.get();
        } else {
            profile = ContainerProfile.builder()
                    .containerId(result.getContainerId())
                    .firstSeen(Instant.now())
                    .totalEvents(0L)
                    .totalAnomalies(0L)
                    .anomalyRate(0.0)
                    .avgAnomalyScore(0.0)
                    .maxAnomalyScore(0.0)
                    .riskLevel("LOW")
                    .build();
        }

        profile.setLastSeen(Instant.now());

        long totalEvents = profile.getTotalEvents() + 1;
        profile.setTotalEvents(totalEvents);

        boolean isAnomalous = Boolean.TRUE.equals(result.getIsAnomalous());
        long totalAnomalies = profile.getTotalAnomalies() + (isAnomalous ? 1 : 0);
        profile.setTotalAnomalies(totalAnomalies);
        profile.setAnomalyRate(totalEvents > 0 ? (double) totalAnomalies / totalEvents : 0.0);

        double score = result.getAnomalyScore() != null ? result.getAnomalyScore() : 0.0;
        double newAvg = ((profile.getAvgAnomalyScore() * (totalEvents - 1)) + score) / totalEvents;
        profile.setAvgAnomalyScore(newAvg);
        profile.setMaxAnomalyScore(Math.max(profile.getMaxAnomalyScore(), score));

        profile.setRiskLevel(computeRiskLevel(profile.getAnomalyRate(), profile.getMaxAnomalyScore()));

        containerProfileRepository.upsertProfile(profile);
        log.debug("Updated container profile: containerId={} riskLevel={} anomalyRate={}",
                profile.getContainerId(), profile.getRiskLevel(), profile.getAnomalyRate());
    }

    private String computeRiskLevel(double anomalyRate, double maxScore) {
        if (maxScore >= 0.9 || anomalyRate >= 0.3) return "CRITICAL";
        if (maxScore >= 0.75 || anomalyRate >= 0.15) return "HIGH";
        if (maxScore >= 0.6 || anomalyRate >= 0.05) return "MEDIUM";
        return "LOW";
    }
}
