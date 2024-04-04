package com.example.springbootkafkanishant.service.spark;

import com.example.springbootkafkanishant.model.payload.Trend;
import com.example.springbootkafkanishant.repository.TrendRepository;
import com.example.springbootkafkanishant.repository.TweetRepository;
import org.apache.spark.sql.Encoders;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.apache.commons.text.TextStringBuilder;

import java.sql.*;
import java.util.List;

@Component
public class HourlyTrendsCalculator {
    private final SparkSession sparkSession;
    private final TweetRepository tweetRepository;
    private final TrendRepository trendRepository;
    @Autowired
    public HourlyTrendsCalculator(SparkSession sparkSession , TweetRepository tweetRepository , TrendRepository trendRepository) {
        this.sparkSession = sparkSession;
        this.tweetRepository = tweetRepository;
        this.trendRepository = trendRepository;
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(HourlyTrendsCalculator.class);

    @Scheduled(fixedDelay = 600000) // Execute every 2 minutes (120000 milliseconds)
    public void calculateTrends() {
        LOGGER.info("Calculating hourly trends...");

        // Define JDBC connection properties
        String url = "jdbc:postgresql://localhost:5432/tweetsAnalysis";
        String table = "tweet";
        String user = "nishant";
        String password = "nishant";

//        // Define query for a specific hour
        String sampleCreatedAt = "2020-11-07 00:00:00+05:30";

        Dataset<Row> tweetDF = tweetRepository.loadTweetsForHour(sparkSession , url , user , password , sampleCreatedAt);

        // Tokenize the tweet text
        Tokenizer tokenizer = new Tokenizer().setInputCol("tweet").setOutputCol("words");
        Dataset<Row> wordsDF = tokenizer.transform(tweetDF);

        // Remove stop words from each tweet
        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words")
                .setOutputCol("filteredWords");
        Dataset<Row> filteredWordsDF = remover.transform(wordsDF);

        // Count the occurrence of each word
        Dataset<Row> wordCounts = filteredWordsDF
                .select(org.apache.spark.sql.functions.explode(org.apache.spark.sql.functions.col("filteredWords")).as("word"))
                .groupBy("word")
                .count()
                .orderBy(org.apache.spark.sql.functions.col("count").desc())
                .limit(20);

        // Load stop words into a DataFrame
        Dataset<Row> stopWordsDF = sparkSession.read().text("/home/nishant/Documents/springboot-kafka-nishant/stopwords.txt").toDF("stopword");

        // Remove stop words from word counts
        Dataset<Row> trends = wordCounts
                .join(stopWordsDF, wordCounts.col("word").equalTo(stopWordsDF.col("stopword")), "left_anti");

        // Show the most frequent non-stop words
        System.out.println("Hourly Trends:");
        trends.show(false);

        List<String> trendList = trends.select("word").as(Encoders.STRING()).collectAsList();

        for (String trend : trendList) {
            System.out.println(trend);
            Dataset<Row> filteredTweets = tweetDF.filter(tweetDF.col("tweet").contains(trend));
            performSentimentAnalysis(filteredTweets , trend , sampleCreatedAt);
        }

        // Now you can push this filteredWordCounts DataFrame to PostgreSQL or perform further analysis
    }

    public void performSentimentAnalysis(Dataset<Row> tweetDF, String name , String hour) {
        tweetDF = tweetDF.withColumn("sentiment", tweetDF.col("sentiment").cast(DataTypes.FloatType));

        tweetDF = tweetDF.filter(tweetDF.col("sentiment").isNotNull());

        // Calculate average sentiment
        double avgSentiment = 0;
        List<Row> rows = tweetDF.selectExpr("avg(sentiment)").collectAsList();
        if (!rows.isEmpty()) {
             avgSentiment = rows.get(0).getDouble(0);
            // Further processing using avgSentiment
        }
        System.out.println("Average sentiment: " + avgSentiment + "\n");

        // Find the top 2 most positive and negative tweets
        Dataset<Row> topPositiveTweets = tweetDF.orderBy(tweetDF.col("sentiment").desc()).select("sentiment", "tweet").distinct().limit(2);
        Dataset<Row> topNegativeTweets = tweetDF.orderBy(tweetDF.col("sentiment")).select("sentiment", "tweet").distinct().limit(2);

        // Format data into a table
        TextStringBuilder tableBuilder = new TextStringBuilder();
        tableBuilder.appendln("| Sentiment | Tweet                                   |");
        tableBuilder.appendln("|-----------|-----------------------------------------|");

        for (Row row : topPositiveTweets.collectAsList()) {
            tableBuilder.append("| ").append(row.getFloat(0)).append("   | ").append(row.getString(1)).append(" |").appendNewLine();
        }

        for (Row row : topNegativeTweets.collectAsList()) {
            tableBuilder.append("| ").append(row.getFloat(0)).append("   | ").append(row.getString(1)).append(" |").appendNewLine();
        }

        // Print the table
        System.out.println(tableBuilder.toString());

        // Store data in the database
        Trend trend = new Trend();
        trend.setName(name);
        trend.setHour(hour);
        trend.setSentimentScore(String.valueOf(avgSentiment));
        if (!topPositiveTweets.isEmpty() && topPositiveTweets.count() > 1) {
            trend.setPositive1(topPositiveTweets.collectAsList().get(0).getString(1));
            trend.setPositive2(topPositiveTweets.collectAsList().get(1).getString(1));
        } else {
            trend.setPositive1("");
            trend.setPositive2("");
        }
        if (!topNegativeTweets.isEmpty() && topNegativeTweets.count() > 1) {
            trend.setNegative1(topNegativeTweets.collectAsList().get(0).getString(1));
            trend.setNegative2(topNegativeTweets.collectAsList().get(1).getString(1));
        } else {
            trend.setNegative1("");
            trend.setNegative2("");
        }

        trendRepository.saveTrend(trend);

    }



}