package com.example.springbootkafkanishant.service.spark;

import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class HourlyTrendsCalculator {
    private final SparkSession sparkSession;

    @Autowired
    public HourlyTrendsCalculator(SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HourlyTrendsCalculator.class);

    @Scheduled(fixedDelay = 120000) // Execute every 2 minutes (120000 milliseconds)
    public void calculateTrends() { // Removed static keyword from method signature
        // Create SparkSession
        LOGGER.info("HELLO I am in Calculate");

        // Define JDBC connection properties
        String url = "jdbc:postgresql://localhost:5432/tweetsAnalysis";
        String table = "tweet";
        String user = "nishant";
        String password = "nishant";

        // Define query for a specific hour
        String sampleCreatedAt = "2020-11-07 00:00:00+05:30";

        // Construct SQL query to select tweets for the hour of the sample created_at
        String sqlQuery = "SELECT * FROM tweet " +
                "WHERE to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS+TZH:TZM') >= '" + sampleCreatedAt + "' " +
                "AND to_timestamp(created_at, 'YYYY-MM-DD HH24:MI:SS+TZH:TZM') < (TIMESTAMP '" + sampleCreatedAt + "' + INTERVAL '1 hour')";

        // Connect to PostgreSQL and load data for a specific hour
        Dataset<Row> tweetDF = sparkSession.read() // Use sparkSession instead of spark
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "(" + sqlQuery + ") as tweets")  // Subquery to filter data for the specific hour
                .option("user", user)
                .option("password", password)
                .load();

        // Tokenize the tweet text
        Tokenizer tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("words");
        Dataset<Row> wordsDF = tokenizer.transform(tweetDF);

        // Remove stop words
        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filteredWords");

        Dataset<Row> filteredWordsDF = remover.transform(wordsDF);

        // Count the occurrence of each word
        Dataset<Row> wordCounts = filteredWordsDF
                .select(org.apache.spark.sql.functions.explode(org.apache.spark.sql.functions.col("filteredWords")).as("word")) // Explode the array of words into separate rows
                .groupBy("word")
                .count()
                .orderBy(org.apache.spark.sql.functions.col("count").desc()); // Order by count in descending order

        // Show the most frequent words
        wordCounts.show(false);

    }
    sparkSession.stop(); // Stop the Spark session

}
