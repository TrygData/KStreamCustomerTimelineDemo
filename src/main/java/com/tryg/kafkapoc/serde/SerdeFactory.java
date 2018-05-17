package com.tryg.kafkapoc.serde;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.stereotype.Component;

@Component
public class SerdeFactory {

    public <T> Serde<T> getSerde(Class<T> targetClass) {
        try {
            return Serdes.serdeFrom(targetClass);
        } catch (IllegalArgumentException e) { //Proper input validation should be made instead of catching the exception
            return getJsonSerde(targetClass);
        }
    }

    private <T> Serde<T> getJsonSerde(Class<T> targetClass) {
        return Serdes.serdeFrom(new JsonSerializer<>(), new JsonDeserializer<>(targetClass));
    }
}