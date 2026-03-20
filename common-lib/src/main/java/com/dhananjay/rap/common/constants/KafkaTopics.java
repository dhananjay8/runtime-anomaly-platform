package com.dhananjay.rap.common.constants;

public final class KafkaTopics {

    private KafkaTopics() {}

    public static final String RUNTIME_EVENTS = "runtime-events";
    public static final String RUNTIME_EVENTS_DLT = "runtime-events.DLT";
    public static final String PROCESSED_EVENTS = "processed-events";
    public static final String PROCESSED_EVENTS_DLT = "processed-events.DLT";
    public static final String FEATURE_VECTORS = "feature-vectors";
    public static final String FEATURE_VECTORS_DLT = "feature-vectors.DLT";
    public static final String ANOMALY_RESULTS = "anomaly-results";
    public static final String ANOMALY_RESULTS_DLT = "anomaly-results.DLT";

    public static final class ConsumerGroups {
        private ConsumerGroups() {}

        public static final String INGESTION_GROUP = "ingestion-group";
        public static final String FEATURE_GROUP = "feature-group";
        public static final String ML_GROUP = "ml-group";
        public static final String DETECTION_GROUP = "detection-group";
        public static final String DLQ_PROCESSOR_GROUP = "dlq-processor-group";
    }
}
