package com.cad.dispatch;

import com.cad.dispatch.functions.DeduplicationFn;
import com.cad.dispatch.models.CallEvent;
import org.apache.flink.streaming.api.operators.KeyedProcessOperator;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DispatchLogicTest {

    @Test
    void testDuplicateEventsAreDropped() throws Exception {
        DeduplicationFn dedupFunction = new DeduplicationFn();
        KeyedOneInputStreamOperatorTestHarness<String, CallEvent, CallEvent> harness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        new KeyedProcessOperator<>(dedupFunction),
                        CallEvent::getIdempotencyKey,
                        org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO
                );

        harness.open();

        CallEvent event1 = new CallEvent("CALL-100", "2026-11-15T16:00:00Z", "KEY-ABC", "house_fire", "Sector-4", "Smoke showing side alpha");
        CallEvent duplicateEvent = new CallEvent("CALL-100", "2026-11-15T16:00:01Z", "KEY-ABC", "house_fire", "Sector-4", "Smoke showing side alpha");

        // First event should emit successfully
        harness.processElement(event1, 100L);
        assertEquals(1, harness.extractOutputValues().size());

        // Duplicate event with the same idempotency key should be dropped by stateful filter
        harness.processElement(duplicateEvent, 200L);
        assertEquals(1, harness.extractOutputValues().size(), "Duplicate cellular retry should be suppressed.");

        harness.close();
    }
}