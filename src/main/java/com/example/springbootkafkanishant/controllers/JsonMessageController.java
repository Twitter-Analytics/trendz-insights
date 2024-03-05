package com.example.springbootkafkanishant.controllers;

import com.example.springbootkafkanishant.kakfa.JsonKafkaProducer;
import org.springframework.http.ResponseEntity;
import com.example.springbootkafkanishant.payload.User;

import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController()
@RequestMapping("/api/v1/kafka")
public class JsonMessageController {
    private JsonKafkaProducer kafkaProducer;

    public JsonMessageController(JsonKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @RequestMapping("/publish/json")
    public ResponseEntity<String> publish(@RequestBody User  user){
        kafkaProducer.sendMessage(user);
        return  ResponseEntity.ok("Json Message sent to kafka topic.");

    }
}
