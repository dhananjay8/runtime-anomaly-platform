package com.dhananjay.rap.feature.consumer;

import com.dhananjay.rap.common.constants.KafkaTopics;
import com.dhananjay.rap.common.event.ProcessedEvent;
import com.dhananjay.rap.common.util.JsonUtil;
import com.dhananjay.rap.feature.service.FeatureEngineeringService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProcessedEventConsumer {

    private final FeatureEngineeringService featureEngineeringService;

    @KafkaListener(
            topics = KafkaTopics.PROCESSED_EVENTS,
            groupId = KafkaTopics.ConsumerGroups.FEATURE_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<ConsumerRecord<String, String>> records) {
        log.info("Received batch of {} processed events for feature extraction", records.size());

        for (ConsumerRecord<String, String> record : records) {
            try {
                ProcessedEvent event = JsonUtil.fromJson(record.value(), ProcessedEvent.class);
                featureEngineeringService.accumulateEvent(event);
            } catch (Exception e) {
                log.error("Failed to process event from partition={} offset={}: {}",
                        record.partition(), record.offset(), e.getMessage(), e);
                throw e;
            }
        }

        featureEngineeringService.flushExpiredWindows();
    }
}
