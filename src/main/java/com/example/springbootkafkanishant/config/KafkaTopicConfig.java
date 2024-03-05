package com.example.springbootkafkanishant.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {
    @Bean
    public NewTopic tweetsTopic() {
        return TopicBuilder.name("tweetsTopic").build();
        //.partitions(2);
    }

    public NewTopic tweetsTopicJSON() {
        return TopicBuilder.name("tweetsTopicJSON").build();
        //.partitions(2);
    }
}
