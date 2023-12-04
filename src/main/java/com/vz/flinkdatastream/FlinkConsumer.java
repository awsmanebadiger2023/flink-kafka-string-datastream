package com.vz.flinkdatastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class FlinkConsumer<T> {
     String TOPIC_IN = "adapt-inbound";
     String BOOTSTRAP_SERVER = "adapt-server:9092,adapt-server:9093,adapt-server:9094";
    public void start(StreamExecutionEnvironment env) throws Exception {
        KafkaSource<String> source =   KafkaSource.<String>builder()
                .setBootstrapServers("adapt-server:9092,adapt-server:9093,adapt-server:9094")
                .setTopics("adapt-inbound")
                .setGroupId("adapt-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> dataStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        dataStream.print();
        env.execute("ADAPT-CONSUMER");
    }
}
