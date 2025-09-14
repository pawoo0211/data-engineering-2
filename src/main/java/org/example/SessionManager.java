package org.example;

import org.apache.spark.sql.SparkSession;

public class SessionManager {
    private SparkSession spark;

    public SessionManager() {
        this.spark = SparkSession.builder()
                .master("local[*]")
                .appName("ub_preprocessor")
                .getOrCreate();
    }

    public SparkSession getSparkSession() {
        return spark;
    }
}
