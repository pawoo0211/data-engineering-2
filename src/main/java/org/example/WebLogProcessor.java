package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.example.entity.WebLog;

import static org.apache.spark.sql.functions.*;

public class WebLogProcessor {

    public Dataset<WebLog> mapToWeblog(Dataset<String> originalDataset) {
        return originalDataset.map((MapFunction<String, WebLog>) record -> {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(record, WebLog.class);
        }, Encoders.bean(WebLog.class));
    }

    public Dataset<Row> assignSession(Dataset<WebLog> ublog) {
        WindowSpec windowSpec = Window.partitionBy("userId").orderBy("timestamp");

        Dataset<Row> logWithSession = ublog.withColumn("prev-ts", lag("timestamp", 1).over(windowSpec))
                .withColumn("session_flag", when(col("prev_ts").isNull()
                        .or(expr("unix_timestamp(timestamp) - unix_timestamp(prev_ts) > 1000")), 1)
                        .otherwise(0)
                );

        return logWithSession.withColumn("session_id", sum("session_flag").over(windowSpec));
    }

    public Dataset<Row> groupingLog(Dataset<Row> sessionizedLog) {
        return sessionizedLog.groupBy("user_id", "session_id")
                .agg(collect_list("url").alias("url_path"));
    }

    public Dataset<Row> transformUrlData(Dataset<Row> groupingLog) {
        return groupingLog.withColumn("url_path", concat_ws(",", col("url_path")));
    }
}
