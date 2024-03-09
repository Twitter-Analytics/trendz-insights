package com.example.springbootkafkanishant.controllers;

import com.example.springbootkafkanishant.kakfa.JsonKafkaProducer;
import com.example.springbootkafkanishant.kakfa.KafkaProducer;
import com.example.springbootkafkanishant.payload.Tweet;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

@Component
@EnableScheduling
public class TweetFetcher {

    private JsonKafkaProducer kafkaProducer;

    public TweetFetcher(JsonKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    private static Tweet  getTweet(String []fields) {
        Tweet tweet = new Tweet();
        try
        {
            tweet.setCreated_at(fields[1].trim()); // Assuming "created_at" is at index 1
            tweet.setTweet_id(fields[2].trim()); // Assuming "tweet_id" is at index 2
            tweet.setTweet(fields[3].trim()); // Assuming "tweet" is at index 3
            tweet.setLikes(fields[4].trim()); // Assuming "likes" is at index 4
            tweet.setRetweet_count(fields[5].trim()); // Assuming "retweet_count" is at index 5
            tweet.setUser_id(fields[6].trim()); // Assuming "user_id" is at index 6
            tweet.setUser_followers_count(fields[7].trim()); // Assuming "user_followers_count" is at index 7
            tweet.setUser_location(fields[8].trim()); // Assuming "user_location" is at index 8
        }
        catch (Exception e){

        }
        return tweet;
    }
    @Scheduled(fixedRate = 10000) // Fetch tweets every 10 seconds
    public void fetchTweets() {
        String filePath = "/home/nishant/Desktop/data1.csv";
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                // Split the CSV line by commas
                String[] fields = line.split(",");

                // Skip lines with only one field
                if (fields.length <= 1) {
                    continue;
                }

                kafkaProducer.sendMessage(getTweet(fields));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
