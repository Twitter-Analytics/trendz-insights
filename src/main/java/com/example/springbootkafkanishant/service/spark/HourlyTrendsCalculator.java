package com.example.springbootkafkanishant.service.spark;

import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.StopWordsRemover;
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
    public void calculateTrends() {
        LOGGER.info("Calculating hourly trends...");

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
        Dataset<Row> tweetDF = sparkSession.read()
                .format("jdbc")
                .option("url", url)
                .option("dbtable", "(" + sqlQuery + ") as tweets")
                .option("user", user)
                .option("password", password)
                .load();

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
                .limit(10);

        // Load stop words into a DataFrame
        Dataset<Row> stopWordsDF = sparkSession.read().text("C:\\Users\\sarva\\OneDrive\\Desktop\\PICT\\java_boot_project\\project_inc\\kafka-pub-sub\\stopwords.txt").toDF("stopword");

        // Remove stop words from word counts
        Dataset<Row> filteredWordCounts = wordCounts
                .join(stopWordsDF, wordCounts.col("word").equalTo(stopWordsDF.col("stopword")), "left_anti");

        // Show the most frequent non-stop words
        filteredWordCounts.show(false);

        // Now you can push this filteredWordCounts DataFrame to PostgreSQL or perform further analysis
    }
}
