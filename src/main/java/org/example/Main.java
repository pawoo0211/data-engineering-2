package org.example;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;

public class Main {
    public static void main(String[] args) {
        System.out.println("Flink Start");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("web_log_topic")
                .setGroupId("flink-consumer")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

    }
}
