package com.tryg.kafkapoc.serde;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NoArgsConstructor;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Map;

@NoArgsConstructor
public class JsonDeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> targetClass;

    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;

        objectMapper.configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void configure(Map<String, ?> props, boolean isKey) {
        targetClass = (Class<T>) props.get("targetClass");
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
