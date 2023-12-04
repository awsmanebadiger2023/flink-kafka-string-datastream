package com.vz.flinkdatastream;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

public class FlinkProducer {
    String TOPIC_IN = "adapt-inbound";
    static String BOOTSTRAP_SERVER = "adapt-server:9092,adapt-server:9093,adapt-server:9094";
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        try {
            start(env);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void start(StreamExecutionEnvironment env) throws Exception {
        DataStream<String> dataStream = env.addSource(new DataGenerator());
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVER)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("adapt-inbound")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        dataStream.print();
        dataStream.sinkTo(sink);
        env.execute("FLINK PRODUCER");
    }

    public static class DataGenerator implements SourceFunction<String>{
       boolean flag = true;
        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            int counter =0 ;
            while(true){
                sourceContext.collect("FROM FLINK PRODUCER:"+ counter++);
                System.out.println("FROM FLINK PRODUCER:"+ counter++);
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            boolean flag = false;
        }
    }
}
