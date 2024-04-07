package com.trendzinsights.service.spark;

import com.trendzinsights.model.payload.Tweet;
import com.trendzinsights.repository.TweetRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaStreamProcessor implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamProcessor.class);
    private String topic = "tweetsTopicJSON";
    private final TweetRepository tweetRepository;

    private final SparkSession sparkSession;

    @Autowired
    public KafkaStreamProcessor(SparkSession sparkSession, TweetRepository tweetRepository) {
        this.sparkSession = sparkSession;
        this.tweetRepository = tweetRepository;
    }


    private String predictSentiment(String text) throws IOException {
        try {
            String apiUrl = "http://localhost:5000/predict_sentiment";
            URL url = new URL(apiUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);


            JSONObject requestBody = new JSONObject();
            requestBody.put("text", text);

            try (OutputStream os = con.getOutputStream()) {
                byte[] input = requestBody.toString().getBytes("utf-8");
                os.write(input, 0, input.length);
            }

            int responseCode = con.getResponseCode();


            StringBuilder response = new StringBuilder();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream(), "utf-8"))) {
                    String inputLine;
                    while ((inputLine = in.readLine()) != null) {
                        response.append(inputLine);
                    }
                }
            } else {
                LOGGER.error("HTTP request failed with response code: " + responseCode);
            }

            return response.toString();
        } catch (IOException e) {
            LOGGER.error("Error predicting sentiment: " + e.getMessage());
            throw e;
        }
    }

    public void consume() throws InterruptedException {
        Duration batchInterval = new Duration(5000); // 5 secs
        JavaStreamingContext streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(sparkSession.sparkContext()), batchInterval);

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
            LOGGER.info("I am here !?00");
            ObjectMapper mapper = new ObjectMapper();

            try {
                return mapper.readValue(record.value().toString(), Tweet.class);
            } catch (Exception e) {
                LOGGER.error("Error deserializing Kafka message into Tweet object", e);
                return null;
            }
        });


        stream.foreachRDD(rdd -> {
            rdd.foreach(tweet -> {
                if (tweet != null) {
                    try {
                        String sentiment = predictSentiment(tweet.getTweet());
                        tweetRepository.saveTweetAndSentiment(tweet, sentiment);
                    } catch (IOException e) {
                        LOGGER.error("Error predicting sentiment for tweet: " + tweet.getTweet(), e);
                    }
                }
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}