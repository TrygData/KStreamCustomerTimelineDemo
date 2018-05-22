package com.tryg.kafkapoc.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

public class CollectionDeserializer<T> implements Deserializer<Collection<T>> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private Class<T> itemClass;

    public CollectionDeserializer(Class<T> itemClass) {
        this.itemClass = itemClass;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        itemClass = (Class<T>) configs.get("itemClass");
    }

    @Override
    public Collection<T> deserialize(String topic, byte[] data) {
        CollectionType collectionType = objectMapper.getTypeFactory().constructCollectionType(HashSet.class, itemClass);
        try {
            return data == null ? null : objectMapper.readValue(data, collectionType);
        } catch (IOException e) {
            throw new SerializationException("Error deserializing collection of " + itemClass.getSimpleName() + " from JSON", e);
        }
    }

    @Override
    public void close() {

    }
}
