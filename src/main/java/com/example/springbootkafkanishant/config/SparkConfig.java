package com.example.springbootkafkanishant.config;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Bean
    public SparkSession sparkSession() {
        SparkConf sparkConf = new SparkConf()
                .setAppName("KafkaSparkStreamingETL")
                .setMaster("local[6]"); // Use appropriate master URL for your deployment
        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }
}