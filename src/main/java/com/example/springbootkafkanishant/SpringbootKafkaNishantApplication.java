package com.example.springbootkafkanishant;

import com.example.springbootkafkanishant.repository.TrendRepositaryImplementaion;
import com.example.springbootkafkanishant.repository.TrendRepository;
import com.example.springbootkafkanishant.repository.TweetRepositoryImplementation;
import com.example.springbootkafkanishant.service.spark.HourlyTrendsCalculator;
import com.example.springbootkafkanishant.service.spark.KafkaStreamProcessor;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

@SpringBootApplication
public class SpringbootKafkaNishantApplication implements ApplicationListener<ContextRefreshedEvent> {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootKafkaNishantApplication.class, args);
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
