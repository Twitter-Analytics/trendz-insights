package com.example.springbootkafkanishant;

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
        // This method will be called when the Spring application context is fully initialized
        TweetRepositoryImplementation tweetRepositoryImplementation = new TweetRepositoryImplementation();

        KafkaStreamProcessor jsonKafkaConsumer = new KafkaStreamProcessor(sparkSession , tweetRepositoryImplementation);
        HourlyTrendsCalculator hourlyTrendsCalculator = new HourlyTrendsCalculator(sparkSession);
        try {
            jsonKafkaConsumer.consume();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
