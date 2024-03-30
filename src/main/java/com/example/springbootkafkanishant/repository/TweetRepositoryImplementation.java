package com.example.springbootkafkanishant.repository;


import com.example.springbootkafkanishant.model.payload.Tweet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@Repository
public class TweetRepositoryImplementation implements TweetRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(TweetRepositoryImplementation.class);

    @Override
    public void saveTweet(Tweet tweet) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/tweetsAnalysis", "nishant", "nishant")) {
            String insertQuery = "INSERT INTO tweets (tweet_id, tweet_content) VALUES (?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
                statement.setString(1, tweet.getTweet_id());
                statement.setString(2, tweet.getTweet());
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            LOGGER.error("Error saving tweet to PostgreSQL", e);
        }
    }
}
