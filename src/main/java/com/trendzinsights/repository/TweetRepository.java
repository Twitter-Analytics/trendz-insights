package com.trendzinsights.repository;

import com.trendzinsights.model.payload.Tweet;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Serializable;


public interface TweetRepository extends Serializable {
    void saveTweetAndSentiment(Tweet tweet , String sentiment);
    public Dataset<Row> loadTweetsForHour(SparkSession sparkSession, String url, String user, String password, String sampleCreatedAt);
}