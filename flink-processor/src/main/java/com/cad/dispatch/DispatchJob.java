package com.cad.dispatch;

import com.cad.dispatch.functions.DeduplicationFn;
import com.cad.dispatch.models.CallEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DispatchJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        String bootstrapServers = "localhost:9092";
        String inputTopic = "incoming-911-calls";
        String outputTopic = "unit-dispatch-orders";

        // 1. Configure Kafka Source for incoming JSON Strings
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("cad-flink-processor-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. Configure Kafka Sink for cleaned output events
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(outputTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        ObjectMapper mapper = new ObjectMapper();

        // 3. Build Stream Graph: Source -> JSON Deserialization -> Key by Idempotency Key -> Deduplicate -> Sink
        DataStream<String> callStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka-911-Source");

        callStream
                .map(json -> mapper.readValue(json, CallEvent.class))
                .keyBy(CallEvent::getIdempotencyKey)
                .process(new DeduplicationFn())
                .map(mapper::writeValueAsString)
                .sinkTo(sink);

        env.execute("CAD Real-Time Unit Dispatch Engine");
    }
}