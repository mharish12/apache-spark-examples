package com.h12.spark.pipeline.streaming.structured;

import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

public class KafkaStructuredStreamingPipeline {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("KafkaStructuredStreamingExample")
                .master("local[2]")//For local or dev testing
                .getOrCreate();

        Dataset<Row> kafkaStream = sparkSession.readStream()
                .format("kafka").
                option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "topicA")
                .option("includeHeaders", true)
                .load();

        Dataset<Row> kafkaStreamSelect = kafkaStream.selectExpr("headers", "CAST(key AS STRING)", "CAST(value AS STRING)", "partition", "offset");

        kafkaStreamSelect.map(new Function1<Row, Boolean>() {
            @Override
            public Boolean apply(Row row) {
                RecordHeaders recordHeaders = (RecordHeaders) row.get(0);
                String key = row.getString(1);
                String value = row.getString(2);
                int partition = row.getInt(3);
                long offset = row.getLong(4);
                System.out.printf("%s, %s, %s, %s, %s.%n", partition, offset, key, recordHeaders, value);
                return true;
            }
        }, Encoders.BOOLEAN());

    }
}
