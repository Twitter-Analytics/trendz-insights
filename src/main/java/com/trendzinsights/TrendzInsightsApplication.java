package com.trendzinsights;

import com.trendzinsights.repository.TrendRepositaryImplementaion;
import com.trendzinsights.repository.TweetRepositoryImplementation;
import com.trendzinsights.service.spark.HourlyTrendsCalculator;
import com.trendzinsights.service.spark.KafkaStreamProcessor;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

@SpringBootApplication
public class TrendzInsightsApplication implements ApplicationListener<ContextRefreshedEvent> {

    public static void main(String[] args) {
        SpringApplication.run(TrendzInsightsApplication.class, args);
    }

    @Autowired
    SparkSession sparkSession;
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        TweetRepositoryImplementation tweetRepositoryImplementation = new TweetRepositoryImplementation();
        TrendRepositaryImplementaion trendRepositaryImplementaion = new TrendRepositaryImplementaion();
        KafkaStreamProcessor jsonKafkaConsumer = new KafkaStreamProcessor(sparkSession , tweetRepositoryImplementation);
        HourlyTrendsCalculator hourlyTrendsCalculator = new HourlyTrendsCalculator(sparkSession , tweetRepositoryImplementation , trendRepositaryImplementaion);
        try {
            jsonKafkaConsumer.consume();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
