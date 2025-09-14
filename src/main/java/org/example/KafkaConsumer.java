package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

public class KafkaConsumer {
    private SparkSession spark;

    public KafkaConsumer(SparkSession spark) {
        this.spark = spark;
    }

    public Dataset<String> readFromKafka(String topic) {
        return spark
                .read()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("group.id", "up_preprocessor")
                .option("subscribe", topic)
                .load()
                .selectExpr("CAST(values as STRING)")
                .as(Encoders.STRING());
    }
}
