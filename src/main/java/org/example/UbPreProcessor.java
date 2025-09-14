package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.example.entity.WebLog;

public class UbPreProcessor {
    public static void main(String[] args) {
        System.out.println("UbPreProcessor start");

        // 1. spark session
        SessionManager sessionManager = new SessionManager();
        SparkSession spark = sessionManager.getSparkSession();

        // 2.neo4j setting
        spark.conf().set("spark.neo4j.bolt.url", "bolt://localhost:7687");
        spark.conf().set("spark.neo4j.bolt.user", "neo4j");
        spark.conf().set("spark.neo4j.bolt.password", "password");

        // 3. read from kafka
        KafkaConsumer kafkaConsumer = new KafkaConsumer(spark);
        Dataset<String> originalDataset = kafkaConsumer.readFromKafka("ublog");

        // 4. process
        WebLogProcessor webLogProcessor = new WebLogProcessor();
        Dataset<WebLog> webLogDataset = webLogProcessor.mapToWeblog(originalDataset);
        Dataset<Row> assignLog = webLogProcessor.assignSession(webLogDataset);
        Dataset<Row> groupingLog = webLogProcessor.groupingLog(assignLog);
        Dataset<Row> transformedLog = webLogProcessor.transformUrlData(groupingLog);

        // 5. neo4j data insert

        // 6. output(csv file)
        transformedLog.write().option("header", true)
                .mode("overwrite")
                .csv("/User/jaddy/sampleData");

        // 7. session stop
        spark.close();
    }
}
