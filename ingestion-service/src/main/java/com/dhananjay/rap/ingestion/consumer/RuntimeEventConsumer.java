package com.dhananjay.rap.ingestion.consumer;

import com.dhananjay.rap.common.constants.KafkaTopics;
import com.dhananjay.rap.common.event.RuntimeEvent;
import com.dhananjay.rap.common.util.JsonUtil;
import com.dhananjay.rap.ingestion.service.EventProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class RuntimeEventConsumer {

    private final EventProcessingService eventProcessingService;

    @KafkaListener(
            topics = KafkaTopics.RUNTIME_EVENTS,
            groupId = KafkaTopics.ConsumerGroups.INGESTION_GROUP,
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<ConsumerRecord<String, String>> records) {
        log.info("Received batch of {} runtime events", records.size());

        for (ConsumerRecord<String, String> record : records) {
            try {
                RuntimeEvent event = JsonUtil.fromJson(record.value(), RuntimeEvent.class);
                eventProcessingService.processEvent(event);
            } catch (Exception e) {
                log.error("Failed to process runtime event from partition={} offset={}: {}",
                        record.partition(), record.offset(), e.getMessage(), e);
                throw e;
            }
        }

        log.debug("Successfully processed batch of {} runtime events", records.size());
    }
}
