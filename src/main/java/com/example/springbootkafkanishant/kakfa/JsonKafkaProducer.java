package com.example.springbootkafkanishant.kakfa;

import com.example.springbootkafkanishant.payload.Tweet;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaProducer {

    private  static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaProducer.class);
    private KafkaTemplate<String , Tweet> kafkaTemplate;
    public JsonKafkaProducer(KafkaTemplate<String, Tweet> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public  void sendMessage(Tweet data) {

        LOGGER.info(String.format("Message sent : %s" , data.toString()));
        Message<Tweet> message = MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC , "tweetsTopicJSON")
                .build();
        kafkaTemplate.send(message);
    }


}