
package com.trendzinsights.controller;

import com.trendzinsights.model.payload.Tweet;
import com.trendzinsights.service.kakfa.Producer;
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
            tweet.setCreated_at(fields[1].trim());
            tweet.setTweet_id(fields[2].trim());
            tweet.setTweet(fields[3].trim());
            tweet.setRetweet_count(fields[5].trim());
            tweet.setUser_id(fields[6].trim());
            tweet.setUser_followers_count(fields[7].trim());
            tweet.setUser_location(fields[8].trim());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return tweet;
    }

    @PostConstruct
    public void loadTweetsFromCsv() {
        String filePath = "/home/nishant/Desktop/temp/data3.csv";
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

    @Scheduled(fixedDelay = 500) // Execute every 0.5 second
    public void sendTweetToKafka() {
        int index = tweetIndex.getAndIncrement();
        if (index < tweets.size()) {
            Tweet tweet = tweets.get(index);
            try {
                ObjectMapper objectMapper = new ObjectMapper();
                String json = objectMapper.writeValueAsString(tweet);
                LOGGER.info("Sending tweet to Kafka: {}", json);
                kafkaProducer.sendMessage(json);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
