package com.example.springbootkafkanishant.service.spark;

import com.example.springbootkafkanishant.model.payload.Tweet;
import com.example.springbootkafkanishant.repository.TweetRepository;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaStreamProcessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamProcessor.class);
    private String topic = "tweetsTopicJSON";
    private final TweetRepository tweetRepository;

    public KafkaStreamProcessor(TweetRepository tweetRepository) {
        this.tweetRepository = tweetRepository;
    }

    public void consume() throws InterruptedException {
        Duration batchInterval = new Duration(5000); // 5000 milliseconds = 5 seconds
        JavaStreamingContext streamingContext = new JavaStreamingContext("local[2]", "KafkaStreamingApp", batchInterval);

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
            ObjectMapper mapper = new ObjectMapper();
            try {
                return mapper.readValue(record.value().toString(), Tweet.class);
            } catch (Exception e) {
                LOGGER.error("Error deserializing Kafka message into Tweet object", e);
                return null;
            }
        });

        // Write data to PostgreSQL
        stream.foreachRDD(rdd -> {
            rdd.foreach(tweetRepository::saveTweet);
        });
//        stream.foreachRDD(rdd -> {
//            rdd.foreachPartition(partition -> {
//                try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/tweetsAnalysis", "nishant", "nishant")) {
//                    String insertQuery = "INSERT INTO tweets (tweet_id, tweet_content) VALUES (?, ?)";
//                    try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
//                        while (partition.hasNext()) {
//                            Tweet tweet = partition.next();
//                            if (tweet != null) {
//                                statement.setString(1, tweet.getTweet_id());
//                                statement.setString(2, tweet.getTweet());
//                                statement.addBatch();
//                            }
//                        }
//                        statement.executeBatch();
//                    }
//                } catch (SQLException e) {
//                    LOGGER.error("Error writing data to PostgreSQL", e);
//                }
//            });
//        });

        // Start the streaming context
        streamingContext.start();

        // Wait for the termination of the context
        streamingContext.awaitTermination();
    }
}
