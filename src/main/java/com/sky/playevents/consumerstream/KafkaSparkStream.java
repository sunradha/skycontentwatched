package com.sky.playevents.consumerstream;
/**
 * This is the Driver Class and front door for the Kafka-Spark streaming application. It creates various objects
 * needed for streaming and sets their configurations and then starts the stream. The duration of the stream can
 * be controlled by setting the parameter "sparkStreamTimeout" (in seconds) which is currently set to -1
 * that means forever.
 */

import com.sky.playevents.producers.PlayoutEventRecord;
import com.sky.playevents.configuration.ReadPropertyFile;

import java.util.*;

import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;


public class KafkaSparkStream {

    /**
     * Main method to start the Streaming application
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {

        Properties properties = ReadPropertyFile.loadProperties();

        assert properties != null;
        SparkConf conf = new SparkConf()
                .setAppName(properties.getProperty("appName"))
                .setMaster(properties.getProperty("sparkMaster"));

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext ssc = new JavaStreamingContext(sc,
                Duration.apply(Long.parseLong(properties.getProperty("sparkMicroBatch"))));

        Set<String> topics = Collections.singleton(properties.getProperty("playEventTopic"));
        Map<String, String> kafkaParams = ReadPropertyFile
                .getKafkaStreamProperties(Objects.requireNonNull(ReadPropertyFile.loadProperties()));

        JavaPairInputDStream<String, Object> directKafkaStream = KafkaUtils
                .createDirectStream(ssc, String.class, Object.class, StringDecoder.class,
                        KafkaAvroDecoder.class, kafkaParams, topics);

        directKafkaStream.foreachRDD(rdd -> {
                    rdd.foreach(record -> {
                        ProcessRecords.processRDDRecord((PlayoutEventRecord) record._2);
                    });
                });

        ssc.start();
        ssc.awaitTerminationOrTimeout((Long.parseLong(properties.getProperty("sparkStreamTimeout"))));
    }
}
