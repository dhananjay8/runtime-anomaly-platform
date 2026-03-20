package com.dhananjay.rap.common.util;

import com.dhananjay.rap.common.event.RuntimeEvent;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JsonUtilTest {

    @Test
    void shouldSerializeAndDeserializeRuntimeEvent() {
        RuntimeEvent event = RuntimeEvent.builder()
                .eventId("test-001")
                .timestamp(Instant.parse("2026-03-20T12:00:00Z"))
                .containerId("abc123def456")
                .pid(1234L)
                .ppid(1L)
                .syscall("read")
                .processName("nginx")
                .uid(101L)
                .hostname("worker-01")
                .build();

        String json = JsonUtil.toJson(event);
        assertThat(json).contains("test-001");
        assertThat(json).contains("abc123def456");
        assertThat(json).contains("read");

        RuntimeEvent deserialized = JsonUtil.fromJson(json, RuntimeEvent.class);
        assertThat(deserialized.getEventId()).isEqualTo("test-001");
        assertThat(deserialized.getContainerId()).isEqualTo("abc123def456");
        assertThat(deserialized.getSyscall()).isEqualTo("read");
        assertThat(deserialized.getPid()).isEqualTo(1234L);
    }

    @Test
    void shouldHandleNullFields() {
        RuntimeEvent event = RuntimeEvent.builder()
                .eventId("test-002")
                .containerId("abc123")
                .syscall("write")
                .build();

        String json = JsonUtil.toJson(event);
        RuntimeEvent deserialized = JsonUtil.fromJson(json, RuntimeEvent.class);
        assertThat(deserialized.getDestinationIp()).isNull();
        assertThat(deserialized.getPort()).isNull();
    }

    @Test
    void shouldIgnoreUnknownProperties() {
        String json = "{\"event_id\":\"test\",\"container_id\":\"abc\",\"syscall\":\"read\",\"unknown_field\":\"value\"}";
        RuntimeEvent event = JsonUtil.fromJson(json, RuntimeEvent.class);
        assertThat(event.getEventId()).isEqualTo("test");
    }

    @Test
    void shouldThrowOnInvalidJson() {
        assertThatThrownBy(() -> JsonUtil.fromJson("not-json", RuntimeEvent.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Failed to deserialize");
    }

    @Test
    void shouldProducePrettyJson() {
        RuntimeEvent event = RuntimeEvent.builder()
                .eventId("test-003")
                .containerId("xyz789")
                .syscall("connect")
                .build();

        String pretty = JsonUtil.toPrettyJson(event);
        assertThat(pretty).contains("\n");
        assertThat(pretty).contains("  ");
    }
}
