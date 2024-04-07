package com.trendzinsights.config;

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
                .setMaster("local[6]");
        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }
}