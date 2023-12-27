package com.h12.spark.pipeline.streaming.legacy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class KafkaStreamingPipeline {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("KafkaStreamingPipeline");

        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(30));

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092"); // Add all props

        Collection<String> kafkaTopics = Arrays.asList("topicA");

        JavaInputDStream<ConsumerRecord<String, String>> kafkaStream = KafkaUtils
                .createDirectStream(streamingContext, LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(kafkaTopics, kafkaParams));


        kafkaStream.map(new Function<ConsumerRecord<String, String>, Boolean>() {
            @Override
            public Boolean call(ConsumerRecord<String, String> v1) throws Exception {
                System.out.printf("%s, %s, %s, %s, %s.%n", v1.timestamp(), v1.partition(), v1.offset(), v1.topic(), v1.key());
                return true;
            }
        });

    }
}
