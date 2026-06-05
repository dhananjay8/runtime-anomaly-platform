package com.dhananjay.rap.detection.consumer;

import com.dhananjay.rap.common.constants.KafkaTopics;
import com.dhananjay.rap.common.event.AnomalyResult;
import com.dhananjay.rap.common.util.JsonUtil;
import com.dhananjay.rap.detection.repository.AnomalyRepository;
import com.dhananjay.rap.detection.service.AlertService;
import com.dhananjay.rap.detection.service.ProfileAggregationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnomalyResultConsumer {

    private final AnomalyRepository anomalyRepository;
    private final ProfileAggregationService profileAggregationService;
    private final AlertService alertService;

    @KafkaListener(
            topics = KafkaTopics.ANOMALY_RESULTS,
            groupId = KafkaTopics.ConsumerGroups.DETECTION_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<ConsumerRecord<String, String>> records) {
        log.info("Received batch of {} anomaly results", records.size());

        for (ConsumerRecord<String, String> record : records) {
            try {
                AnomalyResult result = JsonUtil.fromJson(record.value(), AnomalyResult.class);
                anomalyRepository.insertAnomalyResult(
                        result.getResultId(),
                        result.getVectorId(),
                        result.getContainerId(),
                        result.getAnomalyScore(),
                        Boolean.TRUE.equals(result.getIsAnomalous()),
                        result.getSeverity() != null ? result.getSeverity().name() : "LOW",
                        result.getModelVersion(),
                        result.getContributingFeatures() != null ?
                                String.join(",", result.getContributingFeatures()) : null,
                        result.getDescription(),
                        result.getWindowStart(),
                        result.getWindowEnd()
                );

                profileAggregationService.updateProfile(result);

                if (Boolean.TRUE.equals(result.getIsAnomalous())) {
                    log.warn("ANOMALY DETECTED: container={} score={} severity={}",
                            result.getContainerId(), result.getAnomalyScore(), result.getSeverity());
                    alertService.evaluateAndAlert(result);
                }
            } catch (Exception e) {
                log.error("Failed to persist anomaly result from partition={} offset={}: {}",
                        record.partition(), record.offset(), e.getMessage(), e);
                throw e;
            }
        }
    }
}
