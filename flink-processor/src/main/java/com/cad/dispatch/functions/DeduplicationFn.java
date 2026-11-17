package com.cad.dispatch.functions;

import com.cad.dispatch.models.CallEvent;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DeduplicationFn extends KeyedProcessFunction<String, CallEvent, CallEvent> {

    private transient ValueState<Boolean> seenFlagState;

    @Override
    public void open(Configuration parameters) {
        // RocksDB-backed state to track whether an idempotency key has been processed
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("seen-call-state", Boolean.class);
        seenFlagState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(CallEvent value, Context ctx, Collector<CallEvent> out) throws Exception {
        Boolean seen = seenFlagState.value();

        if (seen == null) {
            // First time seeing this idempotency key: mark as seen and emit downstream
            seenFlagState.update(true);
            out.collect(value);
        } else {
            // Duplicate event detected (cellular retry/re-transmission); drop silently
            System.out.println("Duplicate call suppressed: " + value.getIdempotencyKey());
        }
    }
}