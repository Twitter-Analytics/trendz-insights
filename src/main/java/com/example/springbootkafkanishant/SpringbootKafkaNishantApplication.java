package com.example.springbootkafkanishant;
import com.example.springbootkafkanishant.kakfa.JsonKafkaConsumer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;

@SpringBootApplication
public class SpringbootKafkaNishantApplication implements ApplicationListener<ContextRefreshedEvent> {

    public static void main(String[] args) {
        SpringApplication.run(SpringbootKafkaNishantApplication.class, args);
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // This method will be called when the Spring application context is fully initialized
        JsonKafkaConsumer jsonKafkaConsumer = new JsonKafkaConsumer();
        try {
            jsonKafkaConsumer.consume();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
