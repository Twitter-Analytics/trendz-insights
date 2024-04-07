package com.trendzinsights.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public NewTopic tweetsTopicJSON() {
        return TopicBuilder.name("tweetsTopicJSON").build();
        //.partitions(2);
    }
}
