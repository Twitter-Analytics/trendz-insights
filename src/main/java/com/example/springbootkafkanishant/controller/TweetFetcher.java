
package com.example.springbootkafkanishant.controller;

import com.example.springbootkafkanishant.model.payload.Tweet;
import com.example.springbootkafkanishant.service.kakfa.Producer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Component
@EnableScheduling
public class TweetFetcher {
    private static final Logger LOGGER = LoggerFactory.getLogger(TweetFetcher.class);

    private final Producer kafkaProducer;
    private final List<Tweet> tweets = new ArrayList<>();
    private final AtomicInteger tweetIndex = new AtomicInteger(0);

    public TweetFetcher(Producer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    private static Tweet getTweet(String[] fields) {
        Tweet tweet = new Tweet();
        try {
            tweet.setCreated_at(fields[1].trim()); // Assuming "created_at" is at index 1
            tweet.setTweet_id(fields[2].trim()); // Assuming "tweet_id" is at index 2
            tweet.setTweet(fields[3].trim()); // Assuming "tweet" is at index 3
            tweet.setLikes(fields[4].trim()); // Assuming "likes" is at index 4
            tweet.setRetweet_count(fields[5].trim()); // Assuming "retweet_count" is at index 5
            tweet.setUser_id(fields[6].trim()); // Assuming "user_id" is at index 6
            tweet.setUser_followers_count(fields[7].trim()); // Assuming "user_followers_count" is at index 7
            tweet.setUser_location(fields[8].trim()); // Assuming "user_location" is at index 8
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tweet;
    }

    @PostConstruct
    public void loadTweetsFromCsv() {
        String filePath = "C:\\Users\\sarva\\OneDrive\\Desktop\\PICT\\java_boot_project\\project_inc\\kafka-pub-sub\\data1.csv";
        try (CSVReader reader = new CSVReader(new FileReader(filePath))) {
            String[] line;
            while ((line = reader.readNext()) != null) {
                // Create a Tweet object from the CSV fields and add it to the list
                tweets.add(getTweet(line));
            }
        } catch (IOException | CsvValidationException e) {
            e.printStackTrace();
        }
    }

    @Scheduled(fixedDelay = 10000) // Execute every 10 seconds
    public void sendTweetToKafka() {
        int index = tweetIndex.getAndIncrement();
        if (index < tweets.size()) {
            // Get the next tweet from the list
            Tweet tweet = tweets.get(index);

            // Convert the Tweet object to JSON
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                String json = objectMapper.writeValueAsString(tweet);

                // Send the JSON string to Kafka
                LOGGER.info("Sending tweet to Kafka: {}", json);
                kafkaProducer.sendMessage(json);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
