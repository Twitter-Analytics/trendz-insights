package com.trendzinsights.repository;

import com.trendzinsights.model.payload.Tweet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Repository;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

@Repository
public class TweetRepositoryImplementation implements TweetRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(TweetRepositoryImplementation.class);

    @Override
    public void saveTweetAndSentiment(Tweet tweet, String sentiment) {
        try (Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/tweetsAnalysis", "nishant", "nishant")) {
            String insertQuery = "INSERT INTO tweet (created_at , tweet_id, tweet, likes, retweet_count, user_followers_count, sentiment) VALUES (?, ?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(insertQuery)) {
                SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Timestamp createdAtTimestamp = new Timestamp(dateFormat.parse(tweet.getCreated_at()).getTime());
                statement.setTimestamp(1, createdAtTimestamp);
                statement.setString(2, tweet.getTweet_id());
                statement.setString(3, tweet.getTweet());
                statement.setString(4, tweet.getLikes());
                statement.setString(5, tweet.getRetweet_count());
                statement.setString(6, tweet.getUser_followers_count());
                statement.setString(7, sentiment);
                statement.executeUpdate();
            }
        } catch (SQLException | ParseException e) {
            LOGGER.error("Error writing data to PostgreSQL", e);
        }
    }

    public Dataset<Row> loadTweetsForHour(SparkSession sparkSession, String url, String user, String password, String sampleCreatedAt) {
        String sqlQuery = "SELECT * FROM tweet " +
                "WHERE to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS+TZH:TZM') >= '" + sampleCreatedAt + "' " +
                "AND to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS+TZH:TZM') < (TIMESTAMP '" + sampleCreatedAt + "' + INTERVAL '1 hour')";

        return sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "(" + sqlQuery + ") as tweets")
                .option("user", user)
                .option("password", password)
                .load();
    }

}