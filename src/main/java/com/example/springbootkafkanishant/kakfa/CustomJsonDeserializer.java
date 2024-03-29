package com.example.springbootkafkanishant.kakfa;

import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

public class CustomJsonDeserializer<T> extends JsonDeserializer<T> implements Deserializer<T> {

    public CustomJsonDeserializer(Class<T> targetType) {
        // Call the constructor of the superclass with the target type
        super(targetType);

        // Add your trusted packages here
        this.addTrustedPackages("com.example.springbootkafkanishant.payload");
    }
}
