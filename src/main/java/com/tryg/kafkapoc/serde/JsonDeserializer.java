package com.tryg.kafkapoc.serde;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {

    private final ObjectMapper objectMapper;

    private Class<T> targetClass;

    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;

        objectMapper = new ObjectMapper();
        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        targetClass = (Class<T>) configs.get("targetClass");
    }

    @Override
    public T deserialize(String topic, byte[] bytes) {
        try {
            return bytes == null ? null : objectMapper.readValue(bytes, targetClass);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing " + targetClass.getSimpleName() + " from JSON", e);
        }
    }

    @Override
    public void close() {

    }
}
