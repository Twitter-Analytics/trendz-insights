package com.example.springbootkafkanishant.spark;

import com.example.springbootkafkanishant.payload.Tweet;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaStreamProcessor {
    // consumes tweets from Kafka using Spark Streaming
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamProcessor.class);

    private String topic = "tweetsTopicJSON";


    public void consume() throws InterruptedException {
        Duration batchInterval = new Duration(5000); // 5000 milliseconds = 5 seconds
        JavaStreamingContext streamingContext = new JavaStreamingContext("local[2]", "KafkaStreamingApp" , batchInterval);

        Collection<String> topics = Collections.singletonList(topic);
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put(JsonDeserializer.TRUSTED_PACKAGES, "com.example.springbootkafkanishant.payload");
        kafkaParams.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaParams.put("group.id", "consumerGroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        JavaDStream<Tweet> stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics, kafkaParams)
        ).map(record -> {
            // Deserialize the Kafka message into a Tweet object
            ObjectMapper mapper = new ObjectMapper();
            try {
                return mapper.readValue(record.value().toString(), Tweet.class);
            } catch (Exception e) {
                LOGGER.error("Error deserializing Kafka message into Tweet object", e);
                return null; // Or handle the error as per your requirement
            }
        });
        // Process the received tweets
        stream.foreachRDD(rdd -> {
            rdd.foreach(tweet -> {
                if (tweet != null) {
                    LOGGER.info(String.format("Message received: %s", tweet));

                    try (PrintWriter writer = new PrintWriter(new FileWriter("/home/nishant/Documents/springboot-kafka-nishant/src/main/java/com/example/springbootkafkanishant/kakfa/tweets.txt", true))) {
                        writer.println(tweet); // Write the tweet to the file
                    } catch (IOException e) {
                        LOGGER.info("Error writing tweet to file");
                    }
                    // Process the tweet further here
                }
            });
        });

        // Start the streaming context
        streamingContext.start();

        // Wait for the termination of the context
        streamingContext.awaitTermination();
    }
}
