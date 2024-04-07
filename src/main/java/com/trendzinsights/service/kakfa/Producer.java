package com.trendzinsights.service.kakfa;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.stereotype.Service;

@Service
public class Producer {

    private  static final Logger LOGGER = LoggerFactory.getLogger(Producer.class);
    private KafkaTemplate<String , String> kafkaTemplate;
    public Producer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public  void sendMessage(String data) {

//        LOGGER.info(String.format("Message sent : %s" , data.toString()));
        Message<String> message = MessageBuilder
                .withPayload(data)
                .setHeader(KafkaHeaders.TOPIC , "tweetsTopicJSON")
                .build();
        kafkaTemplate.send(message);
    }


}