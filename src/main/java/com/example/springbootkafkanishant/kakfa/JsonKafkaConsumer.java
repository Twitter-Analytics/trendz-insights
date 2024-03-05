package com.example.springbootkafkanishant.kakfa;
import com.example.springbootkafkanishant.payload.User;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class JsonKafkaConsumer {
    private  static final Logger LOGGER = LoggerFactory.getLogger(JsonKafkaConsumer.class);

    @KafkaListener(topics = "tweetsTopicJSON" , groupId = "consumerGroup")
    public void consume(User user){
        LOGGER.info(String.format("JSON message received at consumer :%s" , user.toString() ));
    }
}
