package com.h12.spark.pipeline.streaming.structured;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Function1;

public class KafkaStructuredStreamingPipeline {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("KafkaStructuredStreamingExample")
//                .master("local[2]")//For local or dev testing
                .getOrCreate();

        SparkConf sparkConf = new SparkConf(true);

        Dataset<Row> kafkaStream = sparkSession.readStream()
                .format("kafka")
                .option("subscribe", "topicA")
                .option("includeHeaders", true)
                .option("kafka." + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092")
                .option("kafka.security.protocol", "SASL_PLAINTEXT")
                .option("kafka.sasl.mechanism", "PLAIN")
                .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";")
                .option("kafka.group.id", "my_group")
                .option("maxOffsetPerTrigger", 10000L)
                .option("failOnDataLoss", "false")
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
